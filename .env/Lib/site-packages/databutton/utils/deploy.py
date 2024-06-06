import logging
import os
import subprocess
import tarfile
from fnmatch import fnmatch
from pathlib import Path
from time import sleep
from typing import IO, Generator

import requests

from databutton.utils import ProjectConfig, get_auth_token, get_databutton_login_info


def get_firebase(path: str):
    token = get_auth_token()
    return requests.get(
        f"https://firestore.googleapis.com/v1/projects/databutton/databases/(default)/documents/{path}",
        headers={"Authorization": f"Bearer {token}"},
    )


def push_firebase(path: str):
    pass


def wait_for_firebase_existence(path, retries=10):
    res = get_firebase(path)
    if retries == 0:
        return None
    if res.status_code == 404:
        # This doesn't exist (yet), try again in 2 seconds
        sleep(2)
        return wait_for_firebase_existence(path, retries=retries - 1)
    return res


def get_deployment(deployment_id):
    return wait_for_firebase_existence(f"deployments/{deployment_id}").json()


def get_build_status(build_id: str) -> str:
    json = get_firebase(f"cloud-builds/{build_id}").json()
    return json["fields"]["status"]["stringValue"]


def get_build_id_from_deployment(deployment_id: str) -> str:
    deployment = get_deployment(deployment_id)
    build_id = deployment["fields"]["buildId"]["stringValue"]
    return build_id


def listen_to_build(deployment_id: str) -> Generator[str, None, None]:
    build_id = get_build_id_from_deployment(deployment_id)
    status = get_build_status(build_id)

    prev = status
    yield status
    while status not in ["SUCCESS", "FAILURE", "CANCELLED"]:
        sleep(5)
        status = get_build_status(build_id)
        if status != prev:
            yield status
            prev = status
    yield status


def create_archive(
    tmpfile: IO[bytes], source_dir: Path = Path.cwd(), config: ProjectConfig = None
) -> str:
    ignore_files = config.exclude
    if ".databutton" in ignore_files:
        # We need this for deploying.
        ignore_files.remove(".databutton")
    generated_requirements = False
    if os.path.exists("pyproject.toml") and not os.path.exists("requirements.txt"):
        # This is a poetry project, export requirements.txt from poetry
        subprocess.run(
            "poetry export -f requirements.txt --output requirements.txt --without-hashes",
            shell=True,
        )
        generated_requirements = True

    def exclude_filter(tarinfo: tarfile.TarInfo):
        for ignore in ignore_files:
            if fnmatch(tarinfo.name, ignore):
                return None
        return tarinfo

    with tarfile.open(fileobj=tmpfile, mode="w:gz") as tar:
        for fn in source_dir.iterdir():
            tar.add(fn, arcname=fn.relative_to(source_dir), filter=exclude_filter)
    if generated_requirements:
        os.remove("requirements.txt")

    # Reset the reader so we can get the contents
    tmpfile.flush()
    tmpfile.seek(0)

    return tmpfile


def get_signed_url_for_upload(
    project_id: str, deployment_id: str, databutton_token: str, auth_token: str
):
    get_signed_url = (
        "https://europe-west1-databutton.cloudfunctions.net/upload_project_signer"
    )
    res = requests.post(
        get_signed_url,
        headers={"Authorization": f"Bearer {auth_token}"},
        json={
            "project_id": project_id,
            "deployment_id": deployment_id,
            "databutton_token": databutton_token,
        },
    )
    if not res.ok:
        raise Exception("Could not upload")
    return res.json()["signed_url"]


def upload_archive(config: ProjectConfig, deployment_id: str, tmpfile: IO[bytes]):
    refresh_token = get_databutton_login_info().refreshToken
    signed_url = get_signed_url_for_upload(
        project_id=config.uid,
        deployment_id=deployment_id,
        databutton_token=refresh_token,
        auth_token=get_auth_token(),
    )
    signed_headers = {
        "Authorization": f"Bearer {get_auth_token()}",
        "x-goog-meta-databutton-token": refresh_token,
        "content-type": "application/tar+gzip",
    }

    response = requests.put(signed_url, headers=signed_headers, data=tmpfile)
    if not response.ok:
        logging.error(response.json())
        raise Exception("Could not upload archive")
    return tmpfile
