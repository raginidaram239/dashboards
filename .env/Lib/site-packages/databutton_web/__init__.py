from pathlib import Path

__version__ = '0.16.0'

def get_static_file_path():
    app_path = Path(__file__).parent / "local"
    return str(app_path)
    