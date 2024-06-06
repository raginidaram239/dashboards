import importlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dataclasses_json import dataclass_json

import databutton as db
from databutton.decorators.apps.streamlit import StreamlitApp
from databutton.decorators.jobs.schedule import DatabuttonSchedule
from databutton.helpers import parse
from databutton.kubernetes.generate import generate_manifest
from databutton.utils import get_databutton_components_path
from databutton.version import __version__


@dataclass_json
@dataclass
class ArtifactDict:
    streamlit_apps: List[StreamlitApp] = field(default_factory=List)
    schedules: List[DatabuttonSchedule] = field(default_factory=List)

    def __eq__(self, other: "ArtifactDict"):
        if isinstance(other, ArtifactDict):
            return self.to_json() == other.to_json()
        return False


def generate_artifacts_json():
    # Sort the apps so that the port proxy remains stable
    for i, st in enumerate(sorted(db.apps._streamlit_apps, key=lambda x: x.route)):
        st.port = 8501 + i
    artifacts = ArtifactDict(
        streamlit_apps=[st for st in db.apps._streamlit_apps],
        schedules=[sched for sched in db.jobs._schedules],
    )
    return artifacts


def write_artifacts_json(artifacts: ArtifactDict):
    if get_databutton_components_path().exists():
        existing = read_artifacts_json()
        if existing == artifacts:
            return
    with open(get_databutton_components_path(), "w") as f:
        f.write(artifacts.to_json(indent=2))


def read_artifacts_json() -> ArtifactDict:
    with open(get_databutton_components_path(), "r") as f:
        return ArtifactDict.from_json(f.read())


def generate_components(rootdir: Path = Path.cwd()):
    normalized_rootdir = rootdir.resolve().relative_to(Path.cwd())
    if rootdir.resolve() not in sys.path:
        sys.path.insert(0, str(rootdir.resolve()))
    # Find all directive modules and import them
    imports = parse.find_databutton_directive_modules(rootdir=normalized_rootdir)

    # Clean the existing artifacts, generate new one
    # TODO: Have a cache mechanism to improve performance
    # shutil.rmtree(Path(".databutton"), ignore_errors=True)
    Path(".databutton").mkdir(exist_ok=True)
    decorator_modules = {}
    for name in imports:
        mod = importlib.import_module(name)
        decorator_modules[name] = mod
    # Write the artifacts
    # Sort the apps so that the port proxy remains stable
    artifacts = generate_artifacts_json()
    write_artifacts_json(artifacts)

    # Copy the Dockerfile
    parent_folder = Path(__file__).parent.parent
    current_dockerfile_path = Path(parent_folder, "docker", "Dockerfile")

    docker_folder_path = Path(".databutton", "docker")
    docker_folder_path.mkdir(exist_ok=True, parents=True)
    dest_dockerfile_path = Path(docker_folder_path, "Dockerfile")
    with open(current_dockerfile_path, "r") as original:
        contents = original.read()
        with open(dest_dockerfile_path, "w") as dest:
            dest.write(
                # Overwrite image
                contents.replace("REPLACE_ME_VERSION", __version__)
            )

    # Generate a kubernetes manifest
    generate_manifest(artifacts)

    # Find apps that are not used and nuke the remainding time
    app_dir = Path(".databutton", "app")
    if app_dir.exists():
        existing_app_files = list(app_dir.iterdir())
        new_fpaths = [app.filename for app in artifacts.streamlit_apps]
        for f in existing_app_files:
            if str(f) not in new_fpaths:
                f.unlink()

    return artifacts
