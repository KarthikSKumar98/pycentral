
from ..base import NewCentralBase
from ..exceptions import LoginError

class TenantBase(NewCentralBase):
    """
    A tenant-scoped connection owned by MSPBase.

    Not intended to be instantiated directly — obtain instances via
    MSPBase.get_tenant_connection().

    On a 401 response, _renew_token() renews the parent MSP's
    shared token first, then re-exchanges it for a fresh tenant-scoped token,
    keeping both connections in sync.

    Args:
        token_info (dict): Pre-built token_info dict, already resolved — bypasses new_parse_input_args.
        msp_parent (MSPBase): The parent MSP instance that owns this tenant connection.
        tenant_workspace_id (str): The tenant's GLP workspace ID, used as the subject
            in the token exchange request.
        logger (logging.Logger, optional): Shared logger inherited from the parent MSP instance.
    """

    def __init__(self, token_info, msp_parent, tenant_workspace_id, logger):
        self._msp_parent = msp_parent
        self._tenant_workspace_id = tenant_workspace_id
        # Bypass new_parse_input_args — token_info is already fully resolved.
        # Directly initialise the minimal NewCentralBase state we need.
        self.token_info = token_info
        self.token_file_path = None
        self.logger = logger
        self._set_timeouts(msp_parent._timeout, msp_parent._connect_timeout)
        self._initialize_connection_state()
        self._app_routes = self._build_app_routes()
        self.scopes = None
        self._initialize_http_clients()

    def _renew_token(self, token_key):
        """
        Renew the MSP parent token then re-exchange for a fresh tenant token.

        Overrides NewCentralBase._renew_token so that command()'s 401 handler
        uses the MSP tenant renewal chain instead of a direct create_token() call.

        Renewal chain on a 401:

        1. Refresh the parent MSP's unified/new_central token via MSPBase.create_token().
        2. Re-exchange that fresh MSP token for a new tenant-scoped token via MSPBase._exchange_tenant_token().
        3. Update this connection's token_info["unified"]["access_token"] with the new tenant token so the next retry uses it.

        Args:
            token_key (str): The token_info key that received the 401 (always "unified" for tenant connections).

        Raises:
            LoginError: If the MSP parent token renewal or tenant exchange fails.
        """
        self.logger.info(
            f"Tenant connection for '{self._tenant_workspace_id}' received a 401. "
            "Renewing MSP parent token then re-exchanging for a fresh tenant token..."
        )

        # Step 1: renew the MSP parent's unified token
        try:
            self._msp_parent.create_token("unified")
        except LoginError as e:
            msg = (
                f"Failed to renew MSP parent token during tenant '{self._tenant_workspace_id}' "
                f"token refresh: {e}"
            )
            self.logger.error(msg)
            raise LoginError(msg)

        # Step 2: re-exchange the fresh MSP token for a new tenant-scoped token
        # and update this connection's access token so the retry succeeds.
        try:
            new_tenant_token = self._msp_parent._exchange_tenant_token(self._tenant_workspace_id)
            self.token_info["unified"]["access_token"] = new_tenant_token
            self.logger.info(
                f"Tenant token for '{self._tenant_workspace_id}' successfully renewed."
            )
        except Exception as e:
            msg = f"Failed to re-exchange tenant token for '{self._tenant_workspace_id}': {e}"
            self.logger.error(msg)
            raise LoginError(msg)
