# (C) Copyright 2025 Hewlett Packard Enterprise Development LP.
# MIT License

import re
import urllib.parse

CATEGORIES = {
    "configuration": {
        "value": "network-config",
        "type": "configuration",
        "latest": "v1",
    },
    "monitoring": {
        "value": "network-monitoring",
        "type": "monitoring",
        "latest": "v1",
    },
    "troubleshooting": {
        "value": "network-troubleshooting",
        "type": "troubleshooting",
        "latest": "v1",
    },
    "subscriptions": {"value": "subscriptions", "type": "glp", "latest": "v1"},
    "user_management": {"value": "identity", "type": "glp", "latest": "v1"},
    "devices": {"value": "devices", "type": "glp", "latest": "v1"},
    "service_catalog": {
        "value": "service-catalog",
        "type": "glp",
        "latest": "v1",
    },
}


def get_prefix(category="configuration", version="latest"):
    """Generate URL prefix for a given category and version.

    Args:
        category (str, optional): API category name.
        version (str, optional): API version, e.g. "v1", "v1alpha1" or
            "v2beta1". "latest" resolves to the category default ("v1" for
            all categories, including configuration).

    Returns:
        (str): URL prefix in the format "category_value/version/".

    Raises:
        ValueError: If category is not supported or version is invalid.
    """
    if category not in CATEGORIES:
        raise ValueError(
            f"Invalid category: {category}, Supported categories: {list(CATEGORIES.keys())}"
        )
    if version == "latest":
        version = CATEGORIES[category]["latest"]
    elif not isinstance(version, str) or not re.fullmatch(
        r"v\d+((alpha|beta)\d+)?", version
    ):
        raise ValueError(
            f"Invalid version: {version}. Expected 'latest' or a version like 'v1', 'v1alpha1' or 'v1beta1'."
        )
    return f"{CATEGORIES[category]['value']}/{version}/"


def generate_url(
    api_endpoint, category="configuration", version="latest", identifier=None
):
    """Generate complete API URL for a given endpoint, category, and version.

    Args:
        api_endpoint (str): The API endpoint path to append to the URL.
        category (str, optional): API category name.
        version (str, optional): API version, e.g. "v1" or "v1alpha1".
            "latest" resolves to the category default.
        identifier (str or int, optional): Profile/resource identifier (name
            or numeric id, e.g. VLAN id 100 or an SSID name) appended as the
            last path segment. It is URL-encoded (spaces, "/" etc.), so there
            is no need to hand-build paths like "layer2-vlan/<id>".

    Returns:
        (str): Complete API URL in the format
            "category[value]/version/api_endpoint[/identifier]".

    Raises:
        ValueError: If category is not supported or version is invalid.
        TypeError: If api_endpoint is not a string.

    Example:
        >>> generate_url("layer2-vlan", identifier="my vlan")
        'network-config/v1/layer2-vlan/my%20vlan'
        >>> generate_url("layer2-vlan", identifier=100)
        'network-config/v1/layer2-vlan/100'
    """
    prefix = get_prefix(category, version)
    if api_endpoint is not None and not isinstance(api_endpoint, str):
        raise TypeError(
            f"Invalid type: {type(api_endpoint)} for api_endpoint, expected str"
        )
    url = f"{prefix}{api_endpoint}"
    if identifier is not None:
        url += "/" + urllib.parse.quote(str(identifier), safe="")
    return url
