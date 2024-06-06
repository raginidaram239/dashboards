import os
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from databutton.utils import ProjectConfig, get_databutton_config
from databutton.version import __version__

if TYPE_CHECKING:
    from databutton.utils.build import ArtifactDict


def generate_kube_resources(cpu: str, memory: str):
    return {
        "requests": {"memory": memory, "cpu": cpu},
        "limits": {"memory": memory, "cpu": cpu},
    }


kube_request_resources = generate_kube_resources(cpu="1", memory="2056Mi")

KUBE_HEALTHCHECK = {
    "livenessProbe": {
        "httpGet": {"path": "/", "port": "http"},
    },
    "readinessProbe": {"httpGet": {"path": "/", "port": "http"}},
}

_streamlit_args = [
    "--server.address=0.0.0.0",
    "--server.fileWatcherType=none",
    "--server.enableWebsocketCompression=true",
    "--browser.gatherUsageStats=false",
    "--global.dataFrameSerialization=arrow",
    "--server.headless=true",
    "--server.enableCORS=false",
    "--server.enableXsrfProtection=false",
]


def generate_manifest(
    artifacts: "ArtifactDict", yaml_path=Path(".databutton", "kubernetes")
):
    try:
        config = get_databutton_config()
    except Exception:
        config = ProjectConfig(uid="not-set", name="not-set")
    project_id = config.uid

    env_from_secret = {"envFrom": [{"secretRef": {"name": "databutton-secret"}}]}

    IMAGE_NAME = os.environ.get("IMAGE_NAME", "databutton-base")

    streamlit_apps = []
    for app in artifacts.streamlit_apps:
        st_app = {
            "name": f"route-{app.uid}",
            "image": IMAGE_NAME,
            "imagePullPolicy": "IfNotPresent",
            "args": [
                app.filename,
                "--server.port",
                f"{app.port}",
            ]
            + _streamlit_args,
            "ports": [{"containerPort": app.port, "name": "http"}],
            "env": [{"name": "PYTHONPATH", "value": "."}],
            **env_from_secret,
            "command": ["streamlit", "run"],
            **KUBE_HEALTHCHECK,
        }
        st_app["resources"] = generate_kube_resources(cpu=app.cpu, memory=app.memory)
        streamlit_apps.append(st_app)

    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": project_id},
    }

    kustomization = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "namespace": project_id,
        "resources": [
            "namespace.yaml",
            "deployment.yaml",
            "service.yaml",
            "mapping.yaml",
        ],
        "commonLabels": {
            "databutton.com/project_id": project_id,
        },
        "commonAnnotations": {
            "databutton.com/version": __version__,
        },
    }

    scheduler_container = {
        "name": "scheduler",
        "image": IMAGE_NAME,
        "imagePullPolicy": "IfNotPresent",
        "command": ["databutton", "schedule"],
        "env": [{"name": "PYTHONPATH", "value": "."}],
        **env_from_secret,
        "resources": generate_kube_resources(cpu="1", memory="2056Mi"),
    }

    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "databutton",
        },
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": streamlit_apps
                    + [scheduler_container]
                    + [
                        {
                            "name": "main",
                            "image": IMAGE_NAME,
                            "ports": [{"containerPort": 8000, "name": "http"}],
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["databutton", "serve"],
                            "env": [
                                {"name": "PYTHONPATH", "value": "."},
                            ],
                            **env_from_secret,
                            "resources": generate_kube_resources(
                                cpu="0.2", memory="256Mi"
                            ),
                            **KUBE_HEALTHCHECK,
                        }
                    ]
                },
            },
        },
    }

    mapping = {
        "apiVersion": "getambassador.io/v3alpha1",
        "kind": "Mapping",
        "metadata": {"name": "databutton"},
        "spec": {
            "hostname": f"p{project_id}.dbtn.app",
            "service": "databutton",
            "prefix": "/",
            "allow_upgrade": ["websocket"],
        },
    }

    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "databutton"},
        "spec": {"ports": [{"port": 80, "targetPort": 8000, "name": "http"}]},
    }

    files = {
        "deployment": deployment,
        "service": service,
        "mapping": mapping,
        "kustomization": kustomization,
        "namespace": namespace,
    }

    if yaml_path is not None:
        kustomize_path = yaml_path
        kustomize_path.mkdir(exist_ok=True, parents=True)
        for kind, d in files.items():
            with open(kustomize_path / f"{kind}.yaml", "w") as f:
                yaml.safe_dump(d, f, allow_unicode=True)

    return files
