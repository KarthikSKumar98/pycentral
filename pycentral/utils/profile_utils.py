# (C) Copyright 2025 Hewlett Packard Enterprise Development LP.
# MIT License

import warnings

from pycentral.exceptions import ParameterError


# remove persona in 2.x: delete this helper and its callers' persona params
def _resolve_device_function(device_function=None, persona=None):
    """Resolve device_function, accepting the deprecated persona alias.

    Args:
        device_function (str, optional): Device function value.
        persona (str, optional): Deprecated alias for device_function.

    Returns:
        (str or None): The resolved device function.

    Raises:
        ParameterError: If both device_function and persona are provided.
    """
    if persona is None:
        return device_function
    if device_function is not None:
        raise ParameterError(
            "Provide only one of 'device_function' or 'persona' (deprecated)."
        )
    warnings.warn(
        "'persona' is deprecated; use 'device_function'. 'persona' will be removed in the first non-alpha 2.x release.",
        DeprecationWarning,
        stacklevel=3,
    )
    return persona


def validate_local(local):
    """Validate local profile attributes and build the API query parameters.

    Args:
        local (dict or None): Local profile attributes, e.g.
            {"scope_id": 12345, "device_function": "CAMPUS_AP"}. "persona" is
            accepted as a deprecated alias for "device_function". Any other
            keys are passed through unchanged.

    Returns:
        (dict): Query parameters in API (kebab-case) form, e.g.
            {"object-type": "LOCAL", "scope-id": 12345,
            "device-function": "CAMPUS_AP"}, or an empty dict if local is empty.

    Raises:
        ParameterError: If local is not a dictionary, scope_id is not an int,
            device_function is not a str, or both device_function and persona
            are provided.
    """
    if not local:
        return {}
    if not isinstance(local, dict):
        raise ParameterError(
            "Invalid local profile attributes. Please provide a valid dictionary."
        )
    params = dict(local)
    scope_id = params.pop("scope_id", None)
    device_function = _resolve_device_function(
        params.pop("device_function", None),
        params.pop("persona", None),  # remove persona in 2.x
    )
    if not isinstance(scope_id, int) or isinstance(scope_id, bool):
        raise ParameterError(
            "Invalid local profile attributes. Key 'scope_id' must be of type int."
        )
    if not isinstance(device_function, str):
        raise ParameterError(
            "Invalid local profile attributes. Key 'device_function' must be of type str."
        )
    return {
        "object-type": "LOCAL",
        "scope-id": scope_id,
        "device-function": device_function,
        **params,
    }
