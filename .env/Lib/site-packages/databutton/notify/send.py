import os
from typing import Any

import requests

from databutton.utils import get_api_url, get_auth_token, get_databutton_config


def get_api_headers() -> dict[str, str]:
    return {
        "Authorization": "Bearer " + get_auth_token(),
        "x-databutton-release": os.environ.get("DATABUTTON_RELEASE", "localdev"),
    }


def send(message: Any):
    """Internal helper to make request towards notification api."""

    # The idea here is that perhaps we can reuse some notification
    # queue system etc across kinds, e.g. email, slack, etc.
    kind = message.__class__.__name__.lower()

    project_id = get_databutton_config().uid
    url = f"{get_api_url(project_id)}/notify/{kind}"

    headers = get_api_headers()

    # TODO: Use pydantic.BaseModel etc instead of dataclasses_json to avoid .to_dict()?
    body = {"message": message.to_dict()}

    res = requests.post(url, headers=headers, json=body)
    if not res.ok:
        raise Exception(f"Failed to send {kind} notification")
    return
