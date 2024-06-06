import logging
import os

import requests

from databutton.utils import get_api_url, get_auth_token, get_databutton_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def run_soon(job_id: str, trigger: str = "code"):
    # TODO: Introduce a DATABUTTON_COMPONENT_ID env var to figure out if we're called from app or job or perhaps locally?

    project_id = get_databutton_config().uid
    logger.debug(f"run_soon {job_id}")

    url = get_api_url(project_id) + "/jobs/run-soon/{job_id}"
    token = get_auth_token()
    if token is None or token == "":
        raise EnvironmentError("Missing auth token. Are you logged in to databutton?")
    headers = {
        "Authorization": f"Bearer {token}",
        "x-databutton-release": os.environ.get("DATABUTTON_RELEASE"),
    }
    res = requests.post(
        url=url,
        headers=headers,
        json={"trigger": trigger},
    )
    if res.ok:
        logger.info(f"Sucessfully enqueued job {job_id}")
        return res.json()
    else:
        logger.error(f"Failed to enqueue job {job_id} (status code {res.status_code})")
        raise Exception("Failed to enqueue job")
