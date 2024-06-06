#
# Module containing helpers to scan through files
#
import fnmatch
import os
import re
from pathlib import Path

from databutton.utils import DEFAULT_GLOB_EXCLUDE, get_databutton_config

# Find all modules we need to import
# 1. Traverse all sub-directories under rootdir
# 2. Find all python files containing a decorator
# 3. If __init__.py doesn't exist in a subdir with a decorator, create it
# 4. Rename python filename to an importable module name
# 5. Return list of modules to import


def find_databutton_directive_modules(rootdir=Path.cwd()):
    try:
        config = get_databutton_config()
    except FileNotFoundError:
        config = None
    excludes = config.exclude if config else DEFAULT_GLOB_EXCLUDE
    modules_to_import = []
    for root, dirs, files in os.walk(rootdir):
        dirs[:] = [d for d in dirs if d not in excludes]
        for file in files:
            filepath_norm = Path(root, file)
            file_norm = Path(file)
            should_exclude = False
            for exclude in excludes:
                if fnmatch.fnmatch(file_norm, exclude):
                    # Skip this file
                    should_exclude = True
                    break
            if should_exclude:
                continue
            if file.endswith(".py"):
                try:
                    s = open(filepath_norm, "r").read()
                except UnicodeDecodeError as e:
                    print(f"Could not read file {file}")
                    print(e)
                    continue
                decorators = ["apps.streamlit", "jobs.repeat_every"]
                for decorator in decorators:
                    occurences = len(re.findall(rf"@[\w]+.{decorator}", s))
                    if occurences > 0:
                        if root != rootdir:
                            init_file = Path(root) / "__init__.py"
                            if not init_file.exists():
                                open(init_file, "a").close()
                        md = filepath_norm
                        md = Path(md.as_posix().replace("./", ""))
                        md = md.as_posix().replace(".py", "").replace("/", ".")
                        modules_to_import.append(str(md))

    return modules_to_import
