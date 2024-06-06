import base64
import hashlib
import inspect
import os
import pathlib
import pickle
from functools import wraps
from typing import Any, Callable, Dict, ParamSpec, TypeVar, Union


class SentinelType:
    pass


Sentinel = SentinelType()


PICKLE_PROTOCOL = 5


def hash_function(f: Callable) -> bytes:
    """Compute a stable hash of a function.

    Experimental!

    This is intended to detect changes to the
    function that @cache is applied to.
    It ignores filename because these functions
    are usually defined in a top level script
    that varies between each run in databutton.
    """
    h = hashlib.sha256()
    h.update(inspect.getsource(f).encode("utf-8"))
    return h.digest()


def compute_cache_key(f_hash: bytes, args: tuple, kwargs: Dict[str, Any]) -> str:
    """Compute a stable hash of a function execution.

    Experimental!

    This is intended to detect changes in the function
    and its arguments for use with @cache.
    """
    h = hashlib.sha256()
    h.update(f_hash)
    for arg in args:
        h.update(pickle.dumps(arg, protocol=PICKLE_PROTOCOL))
    for k in sorted(kwargs):
        h.update(k.encode("utf-8"))
        h.update(pickle.dumps(kwargs[k], protocol=PICKLE_PROTOCOL))
    return base64.urlsafe_b64encode(h.digest()).decode("utf-8")


_cache: Dict[str, bytes] = {}


def read_from_cache_memory(key: str) -> Union[Any, SentinelType]:
    global _cache
    pickled_value = _cache.get(key, Sentinel)
    if pickled_value is Sentinel:
        return Sentinel
    value = pickle.loads(pickled_value)
    return value


def write_to_cache_memory(key: str, value: Any):
    global _cache
    pickled_value = pickle.dumps(value, protocol=PICKLE_PROTOCOL)
    _cache[key] = pickled_value


def cache_path() -> pathlib.Path:
    p = pathlib.Path.home() / ".cache" / "databutton" / "data"
    p.mkdir(exist_ok=True, parents=True)
    return p


def clear_cache():
    """Delete cache files produced by functions decorated with @cache."""
    p = cache_path()
    for f in p.glob("*"):
        os.remove(str(f.absolute()))


def cache_filename(key: str) -> pathlib.Path:
    return cache_path() / key


def read_from_cache(key: str) -> Union[Any, SentinelType]:
    try:
        with open(cache_filename(key), "rb") as f:
            return pickle.load(f)
    except Exception:
        return Sentinel


def write_to_cache(key: str, value: Any):
    with open(str(cache_filename(key)), "w+b") as f:
        pickle.dump(value, f, protocol=PICKLE_PROTOCOL)


T = TypeVar("T")
P = ParamSpec("P")


def cache(f: Callable[P, T]) -> Callable[P, T]:
    """Cache results from calls to f to disk based on input arguments.

    Uses pickle both on arguments and return value of function,
    all must be of types that support the pickle protocol.

    Also do not use for objects which hold references to state in
    external systems such as database connections, other network sockets,
    open file pointers, temporary session tokens, etc,
    even if the basic types happen to be pickleable.
    """
    f_hash = hash_function(f)

    @wraps(f)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        global _cache
        key = compute_cache_key(f_hash, args, kwargs)
        value: T | SentinelType = read_from_cache(key)
        if value is Sentinel:
            value = f(*args, **kwargs)
            write_to_cache(key, value)
        return value

    return wrapped
