import json
import logging
import os
import pathlib
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import List, Optional, Union

import requests
from packaging.version import Version, parse

from databutton.version import __version__

logger = logging.getLogger("databutton.utils")

DEFAULT_GLOB_EXCLUDE = ["venv", ".venv", "__pycache__", ".databutton", ".git"]


@dataclass
class ProjectConfig:
    uid: str
    name: str
    # List of fnmatch patterns to exclude, similar to .gitignore
    exclude: Optional[List[str]] = field(default_factory=lambda: DEFAULT_GLOB_EXCLUDE)


CONFIG_FILENAME = "databutton.json"


def get_databutton_config_path() -> Path:
    project_directory = pathlib.Path.cwd()
    retries = 1  # Increase to re-enable feature looking in parent directories or clean up code
    for _ in range(retries):
        filepath = project_directory / CONFIG_FILENAME
        if filepath.exists():
            return filepath
        project_directory = project_directory.parent
        if not project_directory.is_relative_to(pathlib.Path.home()):
            break
    raise FileNotFoundError(
        "Could not find databutton.json config file in parent directories."
    )


def get_project_directory() -> Path:
    return get_databutton_config_path().parent


def get_databutton_config() -> ProjectConfig:
    envvar = os.environ.get("DATABUTTON_PROJECT_ID")
    if envvar is not None:
        return ProjectConfig(name="env", uid=envvar)
    with open(get_databutton_config_path(), "r") as f:
        config = json.load(f)
        return ProjectConfig(
            name=config["name"],
            uid=config["uid"],
            exclude=config["exclude"],
        )


def get_databutton_project_id() -> str:
    return get_databutton_config().uid


_cached_databutton_config: Optional[ProjectConfig] = None


def get_cached_databutton_config() -> ProjectConfig:
    global _cached_databutton_config
    if _cached_databutton_config is None:
        _cached_databutton_config = get_databutton_config()
    return _cached_databutton_config


def create_databutton_config(
    name: str, uid: str, project_directory: Optional[Path] = None
) -> ProjectConfig:
    if project_directory is None:
        project_directory = pathlib.Path.cwd()
    config = ProjectConfig(
        name=name,
        uid=uid,
        exclude=DEFAULT_GLOB_EXCLUDE,
    )
    with open(project_directory / CONFIG_FILENAME, "w") as f:
        f.write(json.dumps(config.__dict__, indent=2))
        return config


def get_api_url(project_id: str) -> str:
    u = os.environ.get("DATABUTTON_API_URL")
    return u or f"https://p{project_id}.dbtn.app/dbtn"


@dataclass
class LoginData:
    refreshToken: str
    uid: str


def get_databutton_login_info() -> Optional[LoginData]:
    if "DATABUTTON_TOKEN" in os.environ:
        return LoginData(refreshToken=os.environ["DATABUTTON_TOKEN"], uid="token")

    auth_path = get_databutton_login_path()
    auth_path.mkdir(exist_ok=True, parents=True)

    uids = [f for f in os.listdir(auth_path) if f.endswith(".json")]
    if len(uids) > 0:
        # Just take a random one for now
        with open(auth_path / uids[0]) as f:
            config = json.load(f)
            return LoginData(uid=config["uid"], refreshToken=config["refreshToken"])
    return None


def get_databutton_login_path():
    return Path(
        os.environ.get(
            "DATABUTTON_LOGIN_PATH", Path(Path.home(), ".config", "databutton")
        )
    )


def get_databutton_components_path():
    return Path(".databutton", "artifacts.json")


FIREBASE_API_KEY = "AIzaSyAdgR9BGfQrV2fzndXZLZYgiRtpydlq8ug"

_cached_auth_token = None


def get_auth_token() -> str:
    global _cached_auth_token
    # This has a 15 minute cache
    if _cached_auth_token is not None and time() - _cached_auth_token[0] > 60 * 15:
        _cached_auth_token = None
    if _cached_auth_token is None:
        login_info = get_databutton_login_info()
        if login_info is None:
            raise Exception(
                "Could not find any login information."
                "\nAre you sure you are logged in?"
            )
        res = requests.post(
            f"https://securetoken.googleapis.com/v1/token?key={FIREBASE_API_KEY}",
            {"grant_type": "refresh_token", "refresh_token": login_info.refreshToken},
        )
        if not res.ok:
            raise Exception("Could not authenticate")
        json = res.json()
        _cached_auth_token = (time(), json["id_token"])
    return _cached_auth_token[1]


def create_databutton_cloud_project(name: str):
    """Creates a Databutton Cloud Project"""
    token = get_auth_token()

    res = requests.post(
        "https://europe-west1-databutton.cloudfunctions.net/createOrUpdateProject",
        json={"name": name},
        headers={"Authorization": f"Bearer {token}"},
    )

    res_json = res.json()
    new_id = res_json["id"]
    return new_id


def get_build_logs(build_id: str) -> str:
    log_url_response = requests.get(
        "https://europe-west1-databutton.cloudfunctions.net/get_cloud_build_logs",
        params={"build_id": build_id},
        headers={"Authorization": f"Bearer {get_auth_token()}"},
    )
    log_url = log_url_response.json()["signed_url"]
    return log_url


def get_newest_pip_databutton_version() -> Version:
    res = requests.get("https://pypi.python.org/pypi/databutton/json").json()
    v = parse("0")
    for version in res.get("releases").keys():
        vv = parse(version)
        if not vv.is_prerelease:
            v = max(v, vv)
    return v


def new_databutton_version_exists() -> Union[Version, bool]:
    try:
        current = parse(__version__)
        newest = get_newest_pip_databutton_version()
        if newest > current:
            return newest
        return False
    except Exception as e:
        logger.debug("Could not fetch new version", e)


def current_databutton_release():
    return os.environ.get("DATABUTTON_RELEASE", "")


def is_running_locally():
    return current_databutton_release() == ""
