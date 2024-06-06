import datetime
import logging

import requests

from databutton.utils import get_auth_token, get_cached_databutton_config

FIRESTORE_BASE_URL = "https://firestore.googleapis.com/v1/projects/databutton/databases/(default)/documents"

logger = logging.getLogger("databutton.scheduler")


def create_job_run(job_id: str, run_id: str, start_time: str):
    try:
        project_id = get_cached_databutton_config().uid
        res = requests.post(
            f"{FIRESTORE_BASE_URL}/projects/{project_id}/runs",
            params={"documentId": run_id},
            headers={"Authorization": f"Bearer {get_auth_token()}"},
            json={
                "fields": {
                    "jobId": {"stringValue": job_id},
                    "startTime": {"timestampValue": start_time},
                }
            },
        )
        if res.ok:
            data = res.json()
            return data
        else:
            logger.error("Could not create job run")
            logger.error(res.text)
        return None
    except Exception:
        import traceback

        logger.error(traceback.format_exc())
        return None


def update_job_run(
    job_id: str,
    run_id: str,
    start_time: str,
    end_time: str,
    next_run_time: str,
    success: bool,
):
    try:
        project_id = get_cached_databutton_config().uid
        start = datetime_from_iso_utc_timestamp(start_time)
        end = datetime_from_iso_utc_timestamp(end_time)
        duration = int((end - start).microseconds / 1000)
        fields = {
            "jobId": {"stringValue": job_id},
            "startTime": {"timestampValue": start_time},
            "endTime": {"timestampValue": end_time},
            "nextRunTime": {"timestampValue": next_run_time},
            "duration": {"integerValue": duration},
            "success": {"booleanValue": success},
        }
        res = requests.patch(
            f"{FIRESTORE_BASE_URL}/projects/{project_id}/runs/{run_id}",
            headers={"Authorization": f"Bearer {get_auth_token()}"},
            json={
                "fields": fields,
            },
        )
        if res.ok:
            data = res.json()
            return data
        else:
            logger.error("Could not update job run")
            logger.error(res.text)
        return None
    except Exception:
        import traceback

        logger.error(traceback.format_exc())
        return None


def utc_now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def iso_utc_timestamp_now() -> str:
    return utc_now().isoformat() + "Z"


def iso_utc_timestamp(dt: datetime.datetime) -> str:
    return (
        datetime.datetime.fromtimestamp(dt.timestamp(), tz=datetime.timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
        + "Z"
    )


def datetime_from_iso_utc_timestamp(ts: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(ts.removesuffix("Z")).astimezone(
        datetime.timezone.utc
    )


def push_job_log(run_id: str, msg: bool):
    try:
        project_id = get_cached_databutton_config().uid
        res = requests.post(
            f"{FIRESTORE_BASE_URL}/projects/{project_id}/runs/{run_id}/logs",
            headers={"Authorization": f"Bearer {get_auth_token()}"},
            json={
                "fields": {
                    "time": {"timestampValue": iso_utc_timestamp_now()},
                    "msg": {"stringValue": msg},
                }
            },
        )
        if res.ok:
            data = res.json()
            return data
        else:
            logger.error(res.text, res.status_code)
        return None
    except Exception:
        import traceback

        logger.error(traceback.format_exc())
        return None
