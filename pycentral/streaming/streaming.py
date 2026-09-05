import ipaddress
import signal
import threading
import uuid
from urllib.parse import urlencode, urlsplit

import websocket
from google.protobuf import message_factory
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

from .events.alert import alert_pb2
from .events.ap import ap_events_pb2
from .events.audit import audit_trail_pb2
from .events.client import client_pb2
from .events.event import event_pb2
from .events.gateway import gw_pb2
from .events.geofence import geofence_pb2
from .events.location import location_pb2
from .events.location_analytics import location_analytics_pb2
from .events.switch import sw_pb2

_PING_INTERVAL = 10
_PING_TIMEOUT = 5
_GATEWAY_FILTERS = frozenset((
    "com.hpe.greenlake.network-monitoring.v1.gateways.state.device",
    "com.hpe.greenlake.network-monitoring.v1.gateways.state.uplink",
    "com.hpe.greenlake.network-monitoring.v1.gateways.state.vlan",
    "com.hpe.greenlake.network-monitoring.v1.gateways.state.tunnel",
    "com.hpe.greenlake.network-monitoring.v1.gateways.state.interface",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.device",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.uplink",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.uplink_wan",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.uplink_ip_probe",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.tunnel",
    "com.hpe.greenlake.network-monitoring.v1.gateways.stats.interface",
))

# event: (service, version, decoder, allowed filters)
_EVENTS = {
    "audit-trail-events": ("network-services", "v1alpha1", audit_trail_pb2.AuditTrail, None),
    "location": ("network-services", "v1alpha1", location_pb2.StreamLocationMessage, None),
    "rssi-events": ("network-services", "v1alpha1", location_analytics_pb2.RssiEvent, None),
    "geofence": ("network-services", "v1alpha1", geofence_pb2.StreamGeofenceMessage, None),
    "ap-events": ("network-monitoring", "v1alpha1", None, None),
    "clients-events": ("network-monitoring", "v1", client_pb2.StreamClientMessage, None),
    "switch-events": ("network-monitoring", "v1", sw_pb2.StreamSwitchMessage, None),
    "gw-events": ("network-monitoring", "v1", gw_pb2.MonitoringInformation, _GATEWAY_FILTERS),
    "alert-events": ("network-notifications", "v1", alert_pb2.AlertStreamingMessage, None),
}
_AP_MESSAGES = {
    alias: message_factory.GetMessageClass(descriptor)
    for descriptor in ap_events_pb2.DESCRIPTOR.message_types_by_name.values()
    for alias in (descriptor.name, descriptor.full_name, "ap." + descriptor.name)
}


class StreamingDecodeError(ValueError):
    """A streaming frame cannot be decoded for its selected event."""


def get_supported_events():
    """Return the supported Central streaming event names.

    Returns:
        tuple[str, ...]: Immutable, alphabetically ordered event names.
    """
    return tuple(sorted(_EVENTS))


def _resolve_event(event):
    try:
        return _EVENTS[event]
    except KeyError as error:
        raise ValueError(
            f"Unsupported event: {event}. Supported events: {list(get_supported_events())}"
        ) from error


def _is_valid_hostname(hostname):
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        labels = hostname.split(".")
        return bool(labels) and all(
            label and label[0].isalnum() and label[-1].isalnum()
            and all(char.isalnum() or char == "-" for char in label)
            for label in labels
        )


def _normalize_filters(filters, allowed_filters=None):
    if filters is None:
        return None
    if isinstance(filters, str):
        value = filters
    elif isinstance(filters, list) and all(isinstance(item, str) for item in filters):
        value = ",".join(filters)
    else:
        raise ValueError("Filters must be a string or a list of strings.")
    if allowed_filters is not None and not all(
        item in allowed_filters for item in value.split(",")
    ):
        raise ValueError("Unsupported filter for gw-events.")
    return value


def build_streaming_url(base_url, event, filters=None):
    """Build a Central streaming URL without creating a connection.

    Args:
        base_url (str): An HTTPS/WSS origin or a bare hostname, optionally
            with a port and a trailing slash.
        event (str): A value returned by :func:`get_supported_events`.
        filters (str|list[str]|None): Event-type filters. ``gw-events`` only
            accepts the eleven Gateway filters documented by Central.

    Returns:
        str: The WSS endpoint with an encoded ``event-types`` query value.

    Raises:
        ValueError: If the event, origin, or filters are unsupported. Origins
            with credentials, paths, queries, fragments, insecure schemes, or
            invalid hostnames are rejected.
    """
    service, version, _, allowed_filters = _resolve_event(event)
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("base_url must be an HTTPS/WSS origin or hostname.")
    parsed = urlsplit(base_url if "://" in base_url else "//" + base_url)
    if (
        parsed.scheme.lower() not in ("", "https", "wss")
        or not parsed.hostname or parsed.username is not None
        or parsed.password is not None or not _is_valid_hostname(parsed.hostname)
        or parsed.path not in ("", "/") or parsed.query or parsed.fragment
        or "\\" in base_url or any(char.isspace() or ord(char) < 32 for char in base_url)
    ):
        raise ValueError("base_url must be an HTTPS/WSS origin or hostname.")
    try:
        parsed.port
    except ValueError as error:
        raise ValueError("base_url has an invalid port.") from error
    filters = _normalize_filters(filters, allowed_filters)
    url = f"wss://{parsed.netloc}/{service}/{version}/{event}"
    return f"{url}?{urlencode({'event-types': filters})}" if filters else url


def decode_frame(event, frame):
    """Decode a protobuf CloudEvent frame into ``(envelope, payload)``.

    Args:
        event (str): A value returned by :func:`get_supported_events`.
        frame (bytes): Serialized CloudEvent protobuf bytes whose data is a
            protobuf ``Any`` payload for ``event``.

    Returns:
        tuple: The CloudEvent envelope and decoded event payload protobuf.

    Raises:
        StreamingDecodeError: If the event is unsupported, the envelope lacks
            protobuf data, the AP type is unknown, or either protobuf is malformed.
    """
    try:
        _, _, decoder, _ = _resolve_event(event)
    except ValueError as error:
        raise StreamingDecodeError(str(error)) from error
    if not isinstance(frame, (bytes, bytearray)):
        raise StreamingDecodeError("Streaming frames must be bytes.")
    envelope = event_pb2.CloudEvent()
    try:
        envelope.ParseFromString(frame)
    except DecodeError as error:
        raise StreamingDecodeError("Malformed CloudEvent frame.") from error
    if envelope.WhichOneof("data") != "proto_data":
        raise StreamingDecodeError("CloudEvent must contain protobuf data.")
    if decoder is None:
        type_name = envelope.proto_data.type_url.rsplit("/", 1)[-1]
        decoder = _AP_MESSAGES.get(type_name)
        if decoder is None:
            raise StreamingDecodeError(f"Unknown ap-events message type: {type_name}.")
    payload = decoder()
    try:
        payload.ParseFromString(envelope.proto_data.value)
    except DecodeError as error:
        raise StreamingDecodeError("Malformed protobuf payload.") from error
    return envelope, payload


class Streaming:
    """Minimal WebSocket streaming client for Central.

    Responsibilities:
        - Build the WSS URL for the selected streaming endpoint.
        - Maintain a single WebSocket connection with optional auto-reconnect.
        - Decode protobuf payloads and deliver them to a user callback.
        - Allow graceful stop and cleanup of the WebSocket connection.

    Supported events:
        - ``audit-trail-events`` for audit trail updates
        - ``location`` for location updates
        - ``rssi-events`` for RSSI updates
        - ``geofence`` for geofence updates
        - ``ap-events`` for access point updates
        - ``clients-events`` for client updates
        - ``switch-events`` for switch updates
        - ``gw-events`` for gateway updates
        - ``alert-events`` for alert updates

    Args:
        central_conn (NewCentralBase): Central connection object used for
            tokens, base URL, and logging.
        event (str): A supported event name.
        reconnect_delay (int, optional): Delay before reconnecting. Defaults to 5.
        max_retries (int|None, optional): Reconnect limit, or ``None`` forever.
        filters (str|list[str]|None): Event-type filters. Gateway filters are
            validated against Central's published values.
        subscriber_id (str|None): Optional UUIDv4 sent as ``Subscriber-Id``.

    Raises:
        ValueError: If the event, filters, or subscriber ID are invalid.
    """

    def __init__(self, central_conn, event, reconnect_delay=5, max_retries=None,
                 filters=None, subscriber_id=None):
        self.central_conn = central_conn
        self.app_route = central_conn._app_routes["new_central"]
        self.token_key = self.app_route["token_key"]
        self.endpoint = event
        self.service, self.version, self.decoder, allowed_filters = _resolve_event(event)
        self.filters = _normalize_filters(filters, allowed_filters)
        self.subscriber_id = self._validate_subscriber_id(subscriber_id)
        self.reconnect_delay = reconnect_delay
        self.max_retries = max_retries
        self.logger = central_conn.logger
        self.ws = None
        self.user_callback = None
        self.stop_event = threading.Event()
        self._original_sigint = None

    @staticmethod
    def _validate_subscriber_id(subscriber_id):
        if subscriber_id is None:
            return None
        if not isinstance(subscriber_id, str):
            raise ValueError("subscriber_id must be a UUIDv4 string.")
        try:
            value = uuid.UUID(subscriber_id)
        except ValueError as error:
            raise ValueError("subscriber_id must be a UUIDv4 string.") from error
        if value.version != 4 or value.variant != uuid.RFC_4122:
            raise ValueError("subscriber_id must be a UUIDv4 string.")
        return str(value)

    def _build_headers(self):
        """Assemble the HTTP headers required for the WebSocket handshake."""
        token = self.central_conn.token_info[self.token_key]["access_token"]
        headers = [f"Authorization: Bearer {token}"]
        if self.subscriber_id:
            headers.append(f"Subscriber-Id: {self.subscriber_id}")
        return headers

    def _on_message(self, ws, message):
        """Decode a frame and deliver its protobuf-field-name dictionary.

        Invalid frames are logged and skipped so the long-running adapter keeps
        its existing unknown-AP-type behavior.
        """
        try:
            _, decoded_message = decode_frame(self.endpoint, message)
        except StreamingDecodeError as error:
            self.logger.error("Unable to decode %s frame: %s Skipping.", self.endpoint, error)
            return
        json_message = MessageToDict(decoded_message, preserving_proto_field_name=True)
        if self.user_callback:
            try:
                self.user_callback(json_message)
            except Exception as callback_error:
                self.logger.error(f"Callback raised an error: {callback_error}")
        else:
            self.logger.info(f"{json_message}")

    def _on_error(self, ws, error):
        """Handle WebSocket errors.

        * HTTP 401: attempts a token refresh via the Central connection;
          stops streaming if the refresh fails.
        * Other HTTP errors (403, 404, …): unrecoverable — stops streaming
          immediately so the reconnect loop does not retry indefinitely.
        * All other errors (network resets, timeouts, …): logged and left
          for the reconnect loop to handle transparently.

        Args:
            ws (websocket.WebSocketApp): WebSocket instance (unused).
            error (Exception|str): Error raised by the WebSocket client.
        """
        self.logger.error(f"WebSocket error: {error}")
        if isinstance(error, websocket.WebSocketBadStatusException):
            if error.status_code == 401:
                try:
                    self.central_conn._renew_token(self.token_key)
                    self.logger.info("Token refreshed. Will reconnect.")
                except Exception as refresh_error:
                    self.logger.error(f"Token refresh failed: {refresh_error}")
                    self.stop_event.set()
            else:
                # Non-401 HTTP rejection (e.g. 403 Forbidden, 404 Not Found).
                # Retrying will not fix the problem; stop immediately.
                self.logger.error(
                    f"Unrecoverable HTTP error {error.status_code}. "
                    f"Response body: {error.resp_body}. Stopping."
                )
                self.stop_event.set()
        # For all other error types (OSError, network reset, etc.) the
        # reconnect loop in stream() will handle retrying automatically.

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close events.

        Args:
            ws (websocket.WebSocketApp): WebSocket instance (unused).
            close_status_code (int|None): WebSocket close status code.
            close_msg (str|None): Close reason/message from server.
        """
        self.logger.info(
            f"Disconnected (code: {close_status_code}, msg: {close_msg})"
        )

    def _on_open(self, ws):
        """Handle WebSocket open events.

        Args:
            ws (websocket.WebSocketApp): WebSocket instance.
        """
        self.logger.info(
            f"Connection established. Listening for {self.endpoint}..."
        )
        if self.filters:
            self.logger.info(f"Applied filters: {self.filters}")

    def _get_wss_url(self):
        """Build the WSS URL for the configured event without I/O."""
        return build_streaming_url(self.app_route["base_url"], self.endpoint, self.filters)

    def stream(self, callback=None):
        """Start streaming messages for the configured event.

        This method establishes the WebSocket connection, listens for
        messages, and optionally auto-reconnects on unexpected closure
        until `stop()` is called or a fatal error occurs.

        Args:
            callback (callable, optional): Function to be invoked for each
                decoded message. It must accept a single argument
                (dict) representing the decoded protobuf message.
                If not provided, decoded messages are logged.

        Raises:
            KeyboardInterrupt: If interrupted by the user (Ctrl+C) while
                streaming in a foreground loop.
        """
        self.user_callback = callback
        self.stop_event.clear()

        self._setup_signal_handler()

        retry_count = 0
        try:
            while not self.stop_event.is_set():
                try:
                    url = self._get_wss_url()
                    self.ws = websocket.WebSocketApp(
                        url,
                        header=self._build_headers(),
                        on_open=self._on_open,
                        on_close=self._on_close,
                        on_error=self._on_error,
                        on_message=self._on_message,
                    )

                    self.logger.info(f"Connecting to {url.split('?')[0]}...")
                    self.ws.run_forever(
                        ping_interval=_PING_INTERVAL,
                        ping_timeout=_PING_TIMEOUT,
                    )

                    if self.stop_event.is_set():
                        break

                    retry_count += 1
                    if (
                        self.max_retries is not None
                        and retry_count >= self.max_retries
                    ):
                        self.logger.error(
                            f"Max retries ({self.max_retries}) reached. Stopping."
                        )
                        self.stop_event.set()
                        break

                    self.logger.info(
                        f"Connection closed. Reconnecting in {self.reconnect_delay}s… "
                        f"(attempt {retry_count}"
                        + (
                            f"/{self.max_retries}"
                            if self.max_retries is not None
                            else ""
                        )
                        + ")"
                    )
                    if self.stop_event.wait(timeout=self.reconnect_delay):
                        break

                except Exception as e:
                    self.logger.error(
                        f"Unexpected error in streaming loop: {e}"
                    )
                    if self.ws:
                        self.ws.close()
                    retry_count += 1
                    if (
                        self.max_retries is not None
                        and retry_count >= self.max_retries
                    ):
                        self.logger.error(
                            f"Max retries ({self.max_retries}) reached. Stopping."
                        )
                        self.stop_event.set()
                        break
                    if self.stop_event.wait(timeout=self.reconnect_delay):
                        break
        finally:
            self._restore_signal_handler()
            self._cleanup()

    def stop(self):
        """Request the streaming loop to stop and close the WebSocket.

        This method can be called from another thread or from within
        the user callback to stop streaming gracefully.
        """
        self.logger.info("Stop requested.")
        self.stop_event.set()
        self._cleanup()

    def _cleanup(self):
        """Clean up WebSocket resources after streaming stops.

        Ensures the WebSocket is closed and internal references are
        reset. This method is idempotent.
        """
        if self.ws:
            try:
                self.ws.close()
            finally:
                self.ws = None
        self.logger.info("Cleanup complete.")

    def _setup_signal_handler(self):
        """Set up signal handler for graceful shutdown on Ctrl+C."""

        if threading.current_thread() is not threading.main_thread():
            return

        def signal_handler(signum, frame):
            self.logger.info("Received interrupt signal. Stopping...")
            self.stop()

        self._original_sigint = signal.signal(signal.SIGINT, signal_handler)

    def _restore_signal_handler(self):
        """Restore the original signal handler."""
        if threading.current_thread() is not threading.main_thread():
            return
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)
            self._original_sigint = None
