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
    """Validate local profile attributes and prepare them for API requests.

    Args:
        local (dict or None): Local profile attributes dictionary containing
            scope_id (int) and persona (str).

    Returns:
        (dict): Validated local attributes dictionary with object_type set to "LOCAL".

    Raises:
        ParameterError: If local is not a dictionary or missing required keys
            with correct types.
    """
    required_keys = {"scope_id": int, "persona": str}
    local_attributes = dict()
    if local:
        if not isinstance(local, dict):
            raise ParameterError(
                "Invalid local profile attributes. Please provide a valid dictionary."
            )
        for key, expected_type in required_keys.items():
            if key not in local or not isinstance(local[key], expected_type):
                raise ParameterError(
                    f"Invalid local profile attributes. Key '{key}' must be of type {expected_type.__name__}."
                )
        local_attributes = {"object_type": "LOCAL"}
        local_attributes.update(local)
    return local_attributes
