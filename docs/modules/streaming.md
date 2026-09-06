# Streaming

The Central Streaming API pushes events over a WebSocket. `Streaming` connects
to one event stream, decodes each protobuf message, and hands it to your
callback as a dictionary.

## Supported events

| Event | What you receive |
| --- | --- |
| `alert-events` | Alerts |
| `ap-events` | Access point state and statistics |
| `audit-trail-events` | Audit trail entries |
| `clients-events` | Client updates |
| `geofence` | Geofence updates |
| `gw-events` | Gateway state and statistics |
| `location` | Location updates |
| `rssi-events` | RSSI measurements |
| `switch-events` | Switch updates |

`get_supported_events()` returns this list as a tuple.

## Quick start

Create `token.yaml` as described in the
[authentication guide](../getting-started/authentication.md), then:

```python
from pycentral import NewCentralBase
from pycentral.streaming import Streaming

central = NewCentralBase("token.yaml")

def on_message(message):
    print(message)

stream = Streaming(central, "ap-events")
stream.stream(on_message)  # Blocks until stop() is called or Ctrl+C
```

`stream()` reconnects automatically if the connection drops and refreshes the
access token when Central rejects it. Call `stream.stop()` from your callback
or another thread to end the loop.

## Filters

Some events accept an `event-types` filter so you only receive the subtypes you
care about. Pass `filters` as a string or a list of strings:

```python
stream = Streaming(
    central,
    "gw-events",
    filters=["com.hpe.greenlake.network-monitoring.v1.gateways.state.device"],
)
```

The filter values for each event are listed in the
[Streaming API events guide](https://developer.arubanetworks.com/new-central/docs/streaming-api-events).

## Subscriber ID

If several copies of your application should share one stream, give them the
same `subscriber_id` (a UUIDv4). Copies with different IDs each receive every
event. Leave it out for the default behavior. See the
[streaming modes guide](https://developer.arubanetworks.com/new-central/docs/streaming-modes).

```python
stream = Streaming(central, "switch-events", subscriber_id="8b2d2a5c-1c9e-4d3e-9a44-2f5f6b7c8d90")
```

## Reconnect settings

| Argument | Default | Meaning |
| --- | --- | --- |
| `reconnect_delay` | `5` | Seconds to wait before reconnecting |
| `max_retries` | `None` | Reconnects allowed per `stream()` call; `None` means unlimited |

Transient handshake failures (429, 500, 502, 503, 504) and dropped connections
are retried. A 401 triggers a token refresh and a reconnect. Any other HTTP
rejection, such as 403 or 404, stops the stream.

## Using your own WebSocket client

If your application already runs a WebSocket loop, use the helpers directly.
`build_streaming_url()` builds the `wss://` URL and `decode_frame()` turns a
received frame into the CloudEvent envelope and the decoded payload.

```python
from pycentral import NewCentralBase
from pycentral.streaming import build_streaming_url, decode_frame

central = NewCentralBase("token.yaml")
url = build_streaming_url(central.get_base_url(), "switch-events")
headers = {"Authorization": f"Bearer {central.get_access_token()}"}

# Connect to `url` with `headers` using your WebSocket client, then for each
# binary frame received:
envelope, payload = decode_frame("switch-events", frame)
print(envelope.id, payload)
```

`decode_frame()` raises `StreamingDecodeError` if the frame cannot be decoded.
Call `central.refresh_access_token()` when Central rejects the token.

## API reference

::: pycentral.streaming.streaming
