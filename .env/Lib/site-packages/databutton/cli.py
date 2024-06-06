import logging
import os
import shutil
import sys
import tarfile
import tempfile
import webbrowser
from pathlib import Path
from time import sleep
from uuid import uuid4

import click
import requests
import trio
from alive_progress import alive_bar
from uvicorn import Config

from databutton.decorators.jobs.schedule import Scheduler
from databutton.secrets.apiclient import SecretsApiClient
from databutton.secrets.client import get_secrets_client
from databutton.server.dev import DatabuttonRunner
from databutton.utils import (
    LoginData,
    create_databutton_cloud_project,
    create_databutton_config,
    get_api_url,
    get_auth_token,
    get_build_logs,
    get_databutton_config,
    get_databutton_login_info,
    get_databutton_login_path,
    new_databutton_version_exists,
)
from databutton.utils.deploy import (
    create_archive,
    get_build_id_from_deployment,
    listen_to_build,
    upload_archive,
)
from databutton.utils.uvicorn_in_mem import get_threaded_uvicorn
from databutton.version import __version__

logger = logging.getLogger("databutton.cli")

LOGGING_LEVELS = {
    0: logging.NOTSET,
    1: logging.ERROR,
    2: logging.WARN,
    3: logging.INFO,
    4: logging.DEBUG,
}


def require_databutton_login_info() -> LoginData:
    login_info = get_databutton_login_info()
    if login_info is None:
        click.secho("❌ You're not authenticated to Databutton, yet.")
        click.secho(
            f"Type {click.style('databutton login', fg='cyan')} to login or create a user."
        )
        exit(1)
    return login_info


@click.group()
@click.option("--verbose", "-v", count=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx, verbose: int):
    """Run databutton."""
    ctx.ensure_object(dict)
    # Use the verbosity count to determine the logging level...
    if verbose > 0:
        logger.setLevel(
            LOGGING_LEVELS[verbose] if verbose in LOGGING_LEVELS else logging.DEBUG
        )
    ctx.obj["VERBOSE"] = verbose


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))


@cli.command()
@click.argument(
    "project-directory",
    required=True,
)
@click.option(
    "-t",
    "--template",
    required=False,
    default="hello-databutton",
    help="A template to bootstrap off of.",
)
@click.option(
    "-n",
    "--name",
    required=False,
    help="The name of your Databutton project (on databutton.com)",
)
@click.option("--non-interactive", default=False, is_flag=True)
@click.pass_context
def create(
    ctx: click.Context,
    project_directory: Path,
    template: str,
    name: str,
    non_interactive: bool,
):
    """
    Create a Databutton project in the provided project-directory

    PROJECT_DIRECTORY is the name of the directory for your Databutton project.

    Examples:

    \b
    # Create a new Databutton project in the `my-new-project` directory
    databutton create my-new-project

    \b
    # Create a new Databutton project with a custom name
    databutton create my-new-project -n "My new Databutton project"

    \b
    # Create a new Databutton project in the current directory
    databutton create .

    \b
    # Create a new Databutton project from a template
    databutton create my-new-project --template=sales-forecasting

    """
    project_directory = Path(project_directory).resolve()
    project_name = name if name else str(project_directory.as_posix()).split("/")[-1]

    if project_directory.exists():
        if len(list(project_directory.iterdir())) > 0:
            click.secho("❌ The target directory isn't empty.")
            exit(1)
    login_info = get_databutton_login_info()
    if not login_info:
        click.echo()
        click.secho(
            "It doesn't seem like you have a user with Databutton, at least you're not logged in."
        )
        if non_interactive:
            click.echo(
                "Can't log interactively in non-interactive mode."
                + "\nRun databutton login first or run databutton create without --non-interactive"
            )
            exit(1)
        click.secho("Let's log you in or create a user...")
        click.echo()
        ctx.invoke(login)
        login_info = get_databutton_login_info()
    if not login_info:
        # This might still not be true...
        click.echo("Could not log in successfully, please try again..")
        exit(1)
    project_id = create_databutton_cloud_project(project_name)

    click.secho("Downloading template...")
    res = requests.get(
        f"https://storage.googleapis.com/databutton-app-templates/{template}.tar.gz",
        stream=True,
    )
    if not res.ok:
        click.secho(
            f"❌ Failed to get template {template}, are you sure you typed an existing template?"
        )
        exit(1)
    with tempfile.TemporaryFile() as tmpfile:
        tmpfile.write(res.raw.read())
        tmpfile.seek(0)
        with tarfile.open(fileobj=tmpfile) as t:
            t.extractall(project_directory)
    create_databutton_config(
        name=project_name, uid=project_id, project_directory=project_directory
    )
    click.echo()
    click.echo(
        f"Success! Created project {click.style(project_name, fg='cyan')} in {project_directory}"
    )
    click.echo("Inside that directory, you can run several commands:")
    click.echo()

    commands = [
        ("databutton start", "Starts the development server."),
        (
            "databutton deploy",
            "Deploy your project to databutton.com.\n    "
            + "Note that in order to deploy you need a user and a project in Databutton.",
        ),
        (
            "databutton build",
            "Bundles the project and generates the necessary files for production.",
        ),
    ]

    for cmd, desc in commands:
        click.echo(f"  {click.style(cmd, fg='cyan')}")
        click.echo(f"    {click.style(desc)}")
        click.echo()

    click.echo("We suggest you begin by typing:\n")

    cd_dir = (
        project_directory.relative_to(Path.cwd())
        if project_directory.is_relative_to(Path.cwd())
        else project_directory
    )

    click.echo(f"  {click.style('cd', fg='cyan')} {cd_dir}")
    click.secho("  pip install -r requirements.txt", fg="cyan")
    click.secho("  databutton start", fg="cyan")
    click.secho("  databutton deploy", fg="cyan")

    click.echo()
    click.secho(
        f"You can always see the available commands by typing {click.style('databutton --help', fg='cyan')}."
        + f"\nDocumentation is available by typing {click.style('databutton docs', fg='cyan')}."
    )

    click.echo("\nHappy building!\n")


@cli.command()
@click.option("--debug", default=False, type=bool, show_default=True, is_flag=True)
def start(debug=False):
    """Run the Databutton development server"""
    new_version = new_databutton_version_exists()
    if new_version:
        click.echo()
        click.echo("There is a new version of Databutton available")
        click.echo()
        if Path("pyproject.toml").exists():
            # Poetry project
            click.echo(
                f'write {click.style(f"poetry add databutton@{new_version}", fg="yellow")} to install the new version.'
            )
        else:
            click.echo(
                f'write {click.style(f"pip install databutton=={new_version}", fg="yellow")} to install the new version'
            )
        click.echo()
        click.echo()

    # Make sure a config exists
    try:
        get_databutton_config()
    except FileNotFoundError:
        click.secho("Could not find a databutton.json file.")
        click.secho(
            f"Are you sure you ran {click.style('databutton start', fg='cyan')} in the right directory?"
        )
        exit(1)
    except Exception as e:
        print(e)
        exit(1)

    runner = DatabuttonRunner()

    try:
        trio.run(
            runner.run,
            debug,
            # instruments=[Tracer(logger_name="databutton.start")],
        )
    except KeyboardInterrupt:
        logger.debug("Closing scheduler")
        sys.exit(0)


@cli.command()
def login():
    """Login to Databutton"""
    click.echo(click.style("Opening browser to authenticate.."))

    dir_path = Path(__file__).parent
    app_dir = dir_path / "auth"
    sys.path.insert(0, str(app_dir.resolve()))
    login_url = "http://localhost:8008"
    server = get_threaded_uvicorn(
        Config("server:app", port=8008, log_level="error", reload=False)
    )

    # Remove login details
    shutil.rmtree(get_databutton_login_path(), ignore_errors=True)
    with server.run_in_thread():
        webbrowser.open(login_url)
        login_path = get_databutton_login_path()
        while not (os.path.exists(login_path) and len(os.listdir(login_path)) > 0):
            sleep(1)
        # Wait a little bit so the user gets a response
    click.secho("Logged in!")


@cli.command()
@click.pass_context
def deploy(ctx: click.Context):
    """Deploy your project to Databutton"""

    _ = require_databutton_login_info()

    try:
        config = get_databutton_config()
    except FileNotFoundError:
        click.secho(
            "❌ Can't find a Databutton config file. Are you in the correct folder?"
        )
        click.secho(
            f"Type {click.style('databutton init', fg='cyan')} to "
            + "create a new Databutton project in this directory."
        )
        exit(1)

    deployment_id = str(uuid4())

    click.echo()
    click.echo(click.style(f"=== Deploying to {config.name}", fg="green"))
    # ctx.invoke(build)
    click.echo(click.style("i packaging components...", fg="cyan"))

    with tempfile.NamedTemporaryFile() as tmpfile:
        create_archive(tmpfile, source_dir=Path.cwd(), config=config)
        click.echo(click.style("i done packaging components", fg="green"))
        click.echo(click.style("i uploading components", fg="cyan"))
        try:
            upload_archive(config, deployment_id, tmpfile)
        except Exception:
            click.secho("❌ Could not upload, talk to someone at Databutton to debug...")
            exit(1)

        click.echo(click.style("i finished uploading components", fg="green"))
        click.echo(click.style("i cleaning up", fg="cyan"))

    click.echo(
        click.style(
            "i waiting for deployment to be ready, this can take a few minutes...",
            fg="cyan",
        )
    )
    click.secho("i deploying...", fg="cyan")
    click.echo()
    click.secho(
        "i you can close this window if you want, the deploy will continue in the cloud.",
        fg="cyan",
    )

    build_id = get_build_id_from_deployment(deployment_id)
    build_logs = get_build_logs(build_id)
    click.echo()
    click.echo(f"Build logs are available at {build_logs}")
    click.echo()

    with alive_bar(
        stats=False, monitor=False, monitor_end=False, elapsed=False, title=""
    ) as bar:
        for status in listen_to_build(deployment_id):
            bar.text = f"status: {status.capitalize() if status else 'Queued'}"
            bar()

    if status == "SUCCESS":
        click.echo(click.style("✅ Done!", fg="green"))
        click.echo()
        styled_url = click.style(
            f"https://next.databutton.com/projects/{config.uid}", fg="cyan"
        )
        click.secho(f"You can now go to \n\t{styled_url}")
        click.echo()
    elif status == "FAILURE":
        click.echo(click.style("❌ Error deploying...", fg="red"))
        click.echo()
    elif status == "CANCELLED":
        click.secho(
            "Your build was cancelled. A databutler probably did that on purpose."
        )
        click.secho("You should reach out!")


@cli.command()
def build():
    """Build the project, built components will be found in .databutton"""
    click.echo(click.style("i building project", fg="cyan"))
    from databutton.utils.build import generate_components

    click.echo(click.style("i generating components", fg="cyan"))
    artifacts = generate_components()
    click.echo(click.style("i finished building project in .databutton", fg="green"))
    return artifacts


@cli.command()
def serve():
    """Starts a web server for production."""
    click.echo(click.style("=== Serving"))
    import uvicorn

    port = os.environ["PORT"] if "PORT" in os.environ else 8000
    uvicorn.run(
        "databutton.server.prod:app",
        reload=False,
        port=int(port),
        host="0.0.0.0",
    )


@cli.command("init")
@click.option("--name", help="Name of the project")
def init(name: str):
    """Creates a new project in Databutton and writes to databutton.json"""

    _ = require_databutton_login_info()

    if os.path.exists("databutton.json"):
        click.secho("There is already a databutton.json file in this directory")
        project_config = get_databutton_config()
        click.secho(f"  id: {click.style(project_config.uid, fg='cyan')}")
        click.secho(f"  name: {click.style(project_config.name, fg='cyan')}")
        should_overwrite = click.confirm(
            "Do you want to create a new project and overwrite it?", default=True
        )
        if not should_overwrite:
            click.secho("You did not create a new Databutton project.")
            exit(0)
    token = get_auth_token()
    if not name:
        name = click.prompt("Choose a name for your databutton project", type=str)
    res = requests.post(
        "https://europe-west1-databutton.cloudfunctions.net/createOrUpdateProject",
        json={"name": name},
        headers={"Authorization": f"Bearer {token}"},
    )

    res_json = res.json()
    new_id = res_json["id"]
    config = create_databutton_config(name, new_id)
    click.secho(
        f"✅ Created project {name}",
        fg="green",
    )
    click.echo()
    styled_url = click.style(
        f"https://next.databutton.com/projects/{new_id}", fg="cyan"
    )
    click.echo(f"You can check out your project on \n\n  {styled_url}\n")
    click.secho(
        f"Type {click.style('databutton deploy', fg='cyan')} to deploy your project."
    )
    return config


@cli.command()
def docs():
    """Launches https://docs.databutton.com"""
    click.launch("https://docs.databutton.com")


@cli.command()
def logout():
    """Removes all Databutton login info"""
    login_info = get_databutton_login_info()
    if login_info is not None:
        shutil.rmtree(get_databutton_login_path(), ignore_errors=True)
        click.secho("Logged out")
        click.secho(
            f"You can always log in again with {click.style('databutton login', fg='cyan')}"
        )
    else:
        click.secho(
            f"No Databutton user found, did you mean {click.style('databutton login', fg='cyan')}?"
        )


@cli.command()
@click.option("-t", "--token", help="Print an id token", is_flag=True, default=False)
@click.option(
    "-r",
    "--refresh-token",
    default=False,
    is_flag=True,
    help="Print the refresh token."
    + " Set DATABUTTON_TOKEN to this if you want to run databutton in ci or codespaces.",
)
def whoami(token: bool, refresh_token: bool):
    """Shows the logged in user"""
    import jwt

    try:
        id_token = get_auth_token()
    except Exception:
        click.secho("No user found.")
        click.secho(f"Log in first with {click.style('databutton login', fg='cyan')}.")
        exit(1)
    decoded = jwt.decode(id_token, options={"verify_signature": False})
    click.secho("Found logged in user")
    keys_to_print = ["name", "email", "user_id"]
    for k in keys_to_print:
        click.secho(f"  {k}: {click.style(decoded[k], fg='cyan')}")
    if token:
        click.secho(f"  token: {click.style(id_token, fg='cyan')}")
    if refresh_token:
        click.secho(
            f"  refreshToken: {click.style(get_databutton_login_info().refreshToken, fg='cyan')}"
        )
    click.echo()


@cli.command()
def print_token():
    click.echo(get_auth_token())


@cli.command()
def schedule():
    """Starts and runs the scheduler"""
    click.secho("=== Starting the databutton scheduler", fg="cyan")

    Scheduler.create()


@cli.command()
@click.argument("project_id")
@click.argument("project_dir", required=False)
@click.pass_context
def download(ctx: click.Context, project_id: str, project_dir: str):
    """
    Downloads a project locally from PROJECT_ID and into PROJECT_DIR.
    This will download the last deployed code
    """
    project_dir: Path = Path(project_dir) if project_dir is not None else Path.cwd()
    if project_dir.exists():
        if len(list(project_dir.iterdir())) > 0:
            click.secho("")
            click.secho("❌ Can not download into a folder that's not empty.")
            click.secho(
                "Please select a folder that's empty (or one that doesn't exist) :-)"
            )
            exit(1)

    # Double check that the user is logged in
    try:
        get_auth_token()
    except Exception:
        click.echo("It doesn't seem like you're logged in, but we can fix that!")
        if click.confirm("Do you want to log in right now?"):
            ctx.invoke(login)
        else:
            click.echo("You need to be logged in to download this project")
            click.secho(f"Type {click.style('databutton login', fg='cyan')} to login")
            exit(1)

    click.secho("i downloading project...", fg="cyan")
    click.secho(f"i id: {project_id}", fg="cyan")
    click.secho(f"i directory: {str(project_dir)}", fg="cyan")
    download_url = f"{get_api_url(project_id)}/projects/download"
    response = requests.get(
        download_url,
        headers={"Authorization": f"Bearer {get_auth_token()}"},
        stream=True,
    )
    # Ensure that there's a zipped response
    # This can yield a 307 to login if the user does not have access
    # We can catch that by making sure that the response needs to be a tarball
    if (
        not response.ok
        or response.headers.get("content-type") != "application/tar+gzip"
    ):
        # This would be the error message from the upstream /download API
        if "next.databutton.com/login" in response.url:
            # This is what an unauthenticated api requests looks like :'(
            # We get redirected
            click.secho("Are you sure you're logged in as the correct user?")
            click.secho(
                f"Type {click.style('databutton whoami')} to see who you're logged in as."
            )
            exit(1)
        elif "detail" in response.text:
            click.secho("This is the response from the server:")
            click.secho("  " + response.json()["detail"])
        click.echo()
        click.secho(
            f"❌ Failed to get project with id {project_id}, are you sure it's the correct id?"
        )
        click.secho(
            "If you're sure it's the right id, the following url should work and have some components:"
        )
        click.secho(f"https://next.databutton.com/projects/{project_id}", fg="cyan")
        exit(1)
    with tempfile.TemporaryFile() as tmpfile:
        tmpfile.write(response.raw.read())
        tmpfile.seek(0)
        with tarfile.open(fileobj=tmpfile) as t:
            t.extractall(project_dir)
    click.secho("Finished!", fg="green")
    click.echo()
    click.secho("Finished downloading project, we recommmend that you now do")
    click.echo()
    cd_dir = (
        project_dir.relative_to(Path.cwd())
        if project_dir.is_relative_to(Path.cwd())
        else project_dir
    )
    click.secho(f"  cd {cd_dir}", fg="cyan")
    click.secho("  pip install -r requirements.txt", fg="cyan")
    click.secho("  databutton start", fg="cyan")
    click.secho("  and when you're happy with your changes,")
    click.secho("  databutton deploy", fg="cyan")
    click.echo()


@cli.group("secrets")
@click.pass_context
def secrets_group(ctx: click.Context):
    """Manage project secrets"""
    # Shared prerequisites for all secrets commands
    # _ = require_databutton_login_info()
    pass


@secrets_group.command("add")
@click.pass_context
@click.argument("name", required=True)
@click.argument("data-file", type=click.File("r"))
def secrets_add(ctx: click.Context, name: str, data_file):
    """Add secret to project.

    NB! Input is name of secret and the name of a _file_
    containing the secret value.

    The secret value is passed to the databutton api and
    stored encrypted in the cloud.
    """
    secrets_client: SecretsApiClient = get_secrets_client()

    # Read raw bytes from file
    max_len = 64 * 1024
    value: str = data_file.read(max_len + 1)
    if len(value) > max_len:
        click.secho(f"Secret value is too long (max={max_len})")
        exit(1)

    # Remove trailing newline which is usually not part of a secret.
    # Note: If this becomes a problem, add a parameter
    #       to make it opt-out but keep it as default.
    value = value.rstrip("\r\n")

    # Store secret value
    success = secrets_client.add(name, value)
    if success:
        click.echo("Successfully added secret.")
    else:
        click.echo("Failed to add secret!")


@secrets_group.command("delete")
@click.pass_context
@click.argument("name", required=True)
def secrets_delete(ctx: click.Context, name: str):
    """Delete named secret from project"""
    secrets_client: SecretsApiClient = get_secrets_client()

    config = get_databutton_config()
    confirmed = click.confirm(
        f'This will delete the secret "{name}" from project {config.uid}, are you sure?'
    )
    if not confirmed:
        click.echo("Aborted.")

    success = secrets_client.delete(name)
    if success:
        click.echo("Successfully deleted secret.")
    else:
        click.echo("Failed to delete secret!")


@secrets_group.command("get")
@click.argument("name", required=True)
@click.pass_context
def secrets_get(ctx: click.Context, name: str):
    """Show secret value."""
    secrets_client: SecretsApiClient = get_secrets_client()
    value = secrets_client.get(name)
    click.echo(value)


@secrets_group.command("list")
@click.pass_context
def secrets_list(ctx: click.Context):
    """List names of secrets in project"""
    secrets_client: SecretsApiClient = get_secrets_client()
    secrets = secrets_client.list()
    for s in secrets:
        click.echo(s)
