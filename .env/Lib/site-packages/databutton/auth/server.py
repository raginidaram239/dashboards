import json
from pathlib import Path

from databutton_web import get_static_file_path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import FileResponse

from databutton.utils import get_databutton_login_path

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AuthSuccess(BaseModel):
    refreshToken: str
    uid: str


@app.get("/")
def index():
    return FileResponse(f"{get_static_file_path()}/login.html")


@app.post("/")
async def auth_success(auth: AuthSuccess):
    filename = Path(get_databutton_login_path(), f"{auth.uid}.json")
    # Create files with necessary folder structure if it does not exist
    filename.parent.mkdir(exist_ok=True, parents=True)
    with open(filename, "w") as f:
        f.write(
            json.dumps({"uid": auth.uid, "refreshToken": auth.refreshToken}, indent=2)
        )

    return {"success": True}


app_dir = get_static_file_path()
app.mount("/", StaticFiles(directory=app_dir), name="app")
