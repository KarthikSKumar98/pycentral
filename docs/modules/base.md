# Base Module

The base module provides the core connection functionality for PyCentral.

## Connection settings

```python
central = NewCentralBase("token.yaml", timeout=30, connect_timeout=10)
```

`timeout` and `connect_timeout` are in seconds and apply to every request the
connection makes, including token requests.

## Tokens

- `get_base_url()` and `get_access_token()` return the configured values
  without making a network call.
- `refresh_access_token()` requests a new token. When several threads hit an
  expired token at the same time, only one refresh is made.

## Closing

Call `close()` when you are done, or use the connection as a context manager.
Requests made after `close()` raise `RuntimeError`. Stop any `Streaming`
clients before closing the connection they use.

## NewCentralBase

::: pycentral.base.NewCentralBase
