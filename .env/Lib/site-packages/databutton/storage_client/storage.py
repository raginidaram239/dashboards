import base64
import hashlib
import http
import json
import os
import tempfile
from abc import ABC
from collections.abc import Iterable
from enum import Enum
from typing import Any, Optional

import httpx
import jwt
import pandas as pd
from pydantic import BaseModel

from databutton.utils import get_api_url, get_auth_token, get_databutton_project_id
from databutton.utils.utctime import utc_now_str

# This could be tuned
CHUNKSIZE = 4 * 1024 * 1024


class ContentTypes(str, Enum):
    arrow = "vnd.apache.arrow.file"
    json = "application/json"
    text = "text/plain"
    binary = "application/octet-stream"


class ContentShape(BaseModel):
    numberOfRows: int
    numberOfProperties: int


class Serializer(ABC):
    content_type: str = ContentTypes.binary

    def content_shape_of(self, value: Any) -> Optional[ContentShape]:
        return None

    def encode_into(self, value: Any, data_file: tempfile.SpooledTemporaryFile) -> None:
        raise NotImplementedError("Must be implemented by serializer.")

    def decode_from(self, data_file: tempfile.SpooledTemporaryFile) -> Any:
        raise NotImplementedError("Must be implemented by serializer.")

    # TODO: Can probably delete encode/decode now
    def encode(self, value: Any) -> Iterable[bytes]:
        raise NotImplementedError("Must be implemented by serializer.")

    def decode(self, data: Iterable[bytes]) -> Any:
        raise NotImplementedError("Must be implemented by serializer.")


class BinarySerializer(Serializer):
    content_type = ContentTypes.binary

    def encode_into(
        self, value: bytes, data_file: tempfile.SpooledTemporaryFile
    ) -> None:
        data_file.write(value)

    def decode_from(self, data_file: tempfile.SpooledTemporaryFile) -> bytes:
        return data_file.read()

    def encode(self, value: bytes) -> Iterable[bytes]:
        yield value

    def decode(self, data: Iterable[bytes]) -> bytes:
        return b"".join(data)


class TextSerializer(Serializer):
    content_type = ContentTypes.text

    def encode_into(self, value: str, data_file: tempfile.SpooledTemporaryFile) -> None:
        data_file.write(value.encode("utf-8"))

    def decode_from(self, data_file: tempfile.SpooledTemporaryFile) -> str:
        return data_file.read().decode("utf-8", errors="strict")

    def encode(self, value: str) -> Iterable[bytes]:
        yield value.encode("utf-8")

    def decode(self, data: Iterable[bytes]) -> str:
        return b"".join(data).decode(encoding="utf8", errors="strict")


class JsonSerializer(Serializer):
    content_type = ContentTypes.json

    def encode_into(
        self, value: dict, data_file: tempfile.SpooledTemporaryFile
    ) -> None:
        # TODO: Let serializer determine file open more text or binary
        # json.dump(value, data_file)
        data_file.write(json.dumps(value).encode("utf-8"))

    def decode_from(self, data_file: tempfile.SpooledTemporaryFile) -> dict:
        return json.load(fp=data_file)

    def encode(self, value: dict) -> Iterable[bytes]:
        yield json.dumps(value).encode("utf8")

    def decode(self, data: Iterable[bytes]) -> dict:
        return json.loads(b"".join(data))


class DataFrameSerializer(Serializer):
    content_type = ContentTypes.arrow

    def content_shape_of(self, value: pd.DataFrame) -> Optional[ContentShape]:
        rows, cols = value.shape
        return ContentShape(numberOfRows=rows, numberOfProperties=cols)

    def encode_into(
        self, value: pd.DataFrame, data_file: tempfile.SpooledTemporaryFile
    ) -> None:
        value.to_feather(data_file)

    def decode_from(self, data_file: tempfile.SpooledTemporaryFile) -> pd.DataFrame:
        return pd.read_feather(data_file)

    def encode(self, value: pd.DataFrame) -> Iterable[bytes]:
        with tempfile.SpooledTemporaryFile(mode="w+b") as data_file:
            # Serialize entire file, possibly to file if it's large
            value.to_feather(data_file)

            # Yield chunks from file
            data_file.seek(0)
            while chunk := data_file.read(CHUNKSIZE):
                yield chunk

    def decode(self, data: Iterable[bytes]) -> pd.DataFrame:
        with tempfile.SpooledTemporaryFile(mode="w+b") as data_file:
            for chunk in data:
                data_file.write(chunk)
            data_file.seek(0)
            df = pd.read_feather(data_file)
            return df


HTTPX_STORAGE_REQUEST_TIMEOUT = 15.0


class RawStorage:
    """Orchestrate storage of metadata and byte contents on two different stores."""

    def __init__(
        self,
    ):
        self._project_id = None

    def _init(self):
        # Things to do only once
        if self._project_id is None:
            self._project_id = get_databutton_project_id()
            self._dbapi_url = get_api_url(self._project_id)

            # Workaround for local testing:
            self._localdev = self._dbapi_url.startswith("http://localhost")

            self._projectid_param_for_tests = (
                f"?project={self._project_id}" if self._localdev else ""
            )

            # Get userId from auth token or systemid from environment.
            # I.e. which user called, or which scheduled job is running.
            # TODO: This is messy, need to figure out how to do this properly
            system_id = os.environ.get("DATABUTTON_SYSTEM_ID")
            if system_id:
                # If a system is running this, use the provided system id
                user_id = None
                user_name = None
            else:
                # If it's not a system, get user id from auth token,
                # TODO: Use env var for user_id as well, set in devx when calling view/job code?
                # TODO: I don't see how this could be right in devx with multiuser?
                token_claims = jwt.decode(
                    get_auth_token(), options={"verify_signature": False}
                )
                # This is how to extract id and name from the firebase token
                user_id = token_claims.get("sub")
                user_name = token_claims.get("name")

            self._system_id: Optional[str] = system_id
            self._user_id: Optional[str] = user_id
            self._user_name: Optional[str] = user_name

    def _auth_headers(self) -> dict:
        if self._localdev:
            token = "missingtoken"
        else:
            token = get_auth_token()
        return {
            "Authorization": f"Bearer {token}",
        }

    def upload(
        self,
        *,
        data_key: str,
        data_file: tempfile.SpooledTemporaryFile,
        content_length: int,
        content_md5_checksum: str,
        content_type: str,
        content_shape: Optional[ContentShape],
    ):
        self._init()

        def make_prepare_request() -> dict:
            if self._user_id is not None:
                uploaded_by = {
                    "timestamp": utc_now_str(),
                    "type": "user",
                    "id": self._user_id,
                    "name": self._user_name,
                }
            else:
                uploaded_by = {
                    "timestamp": utc_now_str(),
                    "type": "system",
                    "id": self._system_id,
                }
            prepare_request = {
                "uploadedBy": uploaded_by,
                "dataKey": data_key,
                "contentType": content_type,
                "contentLength": content_length,
                "contentMd5Checksum": content_md5_checksum,
            }
            # Extra metadata for dataframes:
            if content_shape is not None:
                prepare_request["contentShape"] = content_shape.dict()
            return prepare_request

        prepare_request = make_prepare_request()

        with httpx.Client() as client:
            # Send metadata to db-api to prepare for blob upload
            resp = client.post(
                url=f"{self._dbapi_url}/storage/prepare{self._projectid_param_for_tests}",
                headers=self._auth_headers(),
                json=prepare_request,
                timeout=HTTPX_STORAGE_REQUEST_TIMEOUT,
            )
            if resp.status_code != http.HTTPStatus.OK:
                raise Exception(
                    f"Upload preparations for '{data_key}' failed, status_code={resp.status_code}"
                )
            prepare_response = resp.json()
            session_url = prepare_response["sessionUrl"]
            blob_key = prepare_response["blobKey"]

        with httpx.Client() as client:
            # Store in blob storage
            # Note: For huge files, using Content-Range headers and looping could be less fragile.
            data_file.seek(0)
            resp = client.put(
                url=session_url,
                headers={
                    "Content-Length": str(content_length),
                },
                content=data_file,
                timeout=HTTPX_STORAGE_REQUEST_TIMEOUT,
            )
            if resp.status_code != http.HTTPStatus.OK:
                raise Exception(
                    f"Upload of '{data_key}' to '{blob_key}' failed, status_code={resp.status_code}"
                )

        # Done uploading!

    def download(
        self,
        *,
        data_key: str,
        data_file: tempfile.SpooledTemporaryFile,
        content_type: str,
    ):
        self._init()

        # Get metadata and download url for current version from db-api
        with httpx.Client() as client:
            resp = client.post(
                url=f"{self._dbapi_url}/storage/geturl{self._projectid_param_for_tests}",
                headers=self._auth_headers(),
                json={
                    "dataKey": data_key,
                    "contentType": content_type,
                },
                timeout=HTTPX_STORAGE_REQUEST_TIMEOUT,
            )
            if resp.status_code == http.HTTPStatus.NOT_FOUND:
                raise FileNotFoundError(f"{data_key} not found")
            if resp.status_code != http.HTTPStatus.OK:
                raise Exception(
                    f"Download preparations for '{data_key}' failed, status_code={resp.status_code}"
                )
            geturl_response = resp.json()
            signed_url = geturl_response["signedUrl"]
            expected_md5_hash = geturl_response["md5"]
            expected_size = geturl_response["size"]

            # Get the bytes from blob store
            resp = client.get(url=signed_url, timeout=HTTPX_STORAGE_REQUEST_TIMEOUT)
            if resp.status_code == http.HTTPStatus.NOT_FOUND:
                raise FileNotFoundError(f"{data_key} not found")
            if resp.status_code != http.HTTPStatus.OK:
                raise Exception(
                    f"Download of '{data_key}' failed, status_code={resp.status_code}, url={signed_url}"
                )
            # Stream response bytes to tempfile in chunks, hash and count while at it
            hashalg = hashlib.md5()
            actual_size = 0
            for chunk in resp.iter_bytes(chunk_size=CHUNKSIZE):
                data_file.write(chunk)
                hashalg.update(chunk)
                actual_size += len(chunk)
            actual_md5_hash = base64.b64encode(hashalg.digest()).decode("utf-8")

            # Sanity checking
            if expected_size != actual_size:
                raise Exception(
                    f"File sizes do not match: {expected_size} != {actual_size}"
                )
            if expected_md5_hash != actual_md5_hash:
                raise Exception(
                    f"Checksums do not match: {expected_md5_hash} != {actual_md5_hash}"
                )
        # Done downloading!


class SerializedStorage:
    """Internal helper class for typed storage managers."""

    def __init__(
        self,
        *,
        raw_storage: RawStorage,
        serializer: Serializer,
    ):
        self._raw_storage = raw_storage
        self.serializer = serializer

    def put(self, key: str, value: Any):
        ser = self.serializer
        with tempfile.SpooledTemporaryFile(mode="w+b") as f:
            # Serialize entire file (possibly to file if it's large)
            ser.encode_into(value, f)

            # Compute size and md5 from serialized bytes
            # (could probably be streamlined to avoid iterating over file again)
            hashalg = hashlib.md5()
            size = 0
            f.seek(0)
            while chunk := f.read(CHUNKSIZE):
                size += len(chunk)
                hashalg.update(chunk)
            md5 = base64.b64encode(hashalg.digest()).decode("utf-8")

            # To implement client side encryption we would add something like this here:
            # 1. generate a random dek (data encryption key)
            # dek = generate_key()
            # 2. encrypt dek with a call to a databutton-keyring service wrapping kms
            # encrypted_dek = encrypt_key(dek)
            # 3. encrypt bytes_value with dek before storing in blob
            # bytes_value = encrypt_user_data(bytes_value, encryption_key)
            # 4. store encrypted dek in metadata

            # Upload from file (or it could still be in memory if it's small)
            f.seek(0)
            self._raw_storage.upload(
                data_key=key,
                data_file=f,
                content_length=size,
                content_md5_checksum=md5,
                content_type=ser.content_type,
                content_shape=ser.content_shape_of(value),
            )

    def get(self, key: str, *, default: Optional[Any] = None) -> Optional[Any]:
        ser = self.serializer
        try:
            with tempfile.SpooledTemporaryFile(mode="w+b") as f:
                # Download into tempfile (if it's small it stays in memory)
                self._raw_storage.download(
                    data_key=key,
                    data_file=f,
                    content_type=ser.content_type,
                )

                # To implement client side encryption we would add this here:
                # 1. get encrypted dek (data encryption key) from geturl metadata
                # 2. decrypt dek with a call to a databutton-keyring service wrapping kms
                # 3. decrypt bytes_value with dek
                # if encrypted_dek is not None:
                #     bytes_value = decrypt_user_data(encrypted_dek, bytes_value, aad=something)

                # Now decode from tempfile
                f.seek(0)
                value = ser.decode_from(f)
        except FileNotFoundError:
            if default is not None:
                if callable(default):
                    return default()
                return default
            raise
        return value


class BinaryStorage:
    """Manage storage of raw binary files."""

    Serializer = BinarySerializer

    def __init__(self, *, raw_storage: RawStorage):
        self._store = SerializedStorage(
            raw_storage=raw_storage,
            serializer=self.Serializer(),
        )

    def put(self, key: str, value: bytes):
        self._store.put(key=key, value=value)

    def get(self, key: str, *, default: Optional[bytes] = None) -> Optional[bytes]:
        return self._store.get(key=key, default=default)


class TextStorage:
    """Manage storage of plain text files."""

    Serializer = TextSerializer

    def __init__(self, *, raw_storage: RawStorage):
        self._store = SerializedStorage(
            raw_storage=raw_storage, serializer=self.Serializer()
        )

    def put(self, key: str, value: str):
        self._store.put(key=key, value=value)

    def get(self, key: str, *, default: Optional[str] = None) -> Optional[str]:
        return self._store.get(key=key, default=default)


class JsonStorage:
    """Manage storage of json files, assumed to be a dict on the python side."""

    Serializer = JsonSerializer

    def __init__(self, *, raw_storage: RawStorage):
        self._store = SerializedStorage(
            raw_storage=raw_storage, serializer=self.Serializer()
        )

    def put(self, key: str, value: dict):
        self._store.put(key=key, value=value)

    def get(self, key: str, *, default: Optional[dict] = None) -> Optional[dict]:
        return self._store.get(key=key, default=default)


class DataFramesStorage:
    """Manage storage of pandas dataframes as arrow files."""

    Serializer = DataFrameSerializer

    def __init__(self, *, raw_storage: RawStorage):
        self._store = SerializedStorage(
            raw_storage=raw_storage, serializer=self.Serializer()
        )

    def put(
        self,
        key: str,
        value: Optional[pd.DataFrame] = None,
        *,
        # This is kept because of backwards-compat
        df: Optional[pd.DataFrame] = None,
        persist_index: bool = False,
    ):
        """Store a dataframe under key in your Databutton project storage.

        Usage:
            db.storage.dataframes.put("key", mydataframe)
            db.storage.dataframes.put(key="key", value=mydataframe)

        Deprecated notation:
            db.storage.dataframes.put(mydataframe, "key")
            db.storage.dataframes.put(key="key", df=mydataframe)
        """
        # Backwards compatibility: Exactly one of value and df must be provided
        if value is not None and df is not None:
            raise ValueError(
                "'df' is provided for backwards compatibility, use only 'value'"
            )
        if value is None and df is None:
            raise ValueError("Missing 'value' argument")

        # Backwards compatibility: If df is provided, use it and ask user to switch
        if value is None:  # implies df is not None because of above checks
            # TODO: Perhaps we should have some sort of a side channel for
            #  deprecation warnings, so we can measure it and also avoid
            #  polluting user code output?
            print("Deprecation warning: use 'value' instead of 'df'.")
            value, df = df, None

        # Backwards compatibility: If (value, key) is provided, swap them and ask user to switch
        if isinstance(key, pd.DataFrame) and isinstance(value, str):
            print("Deprecation warning: Swap put(value, key) -> put(key, value).")
            key, value = value, key

        if not persist_index:
            value = value.reset_index(drop=True)
        self._store.put(key=key, value=value)
        return True  # From old implementation

    def get(
        self,
        key: str,
        *,
        ignore_not_found: bool = True,
        default: Optional[pd.DataFrame] = None,
    ) -> Optional[pd.DataFrame]:
        if default is None and ignore_not_found is True:

            def empty_dataframe() -> pd.DataFrame:
                return pd.DataFrame()

            default = empty_dataframe
        return self._store.get(key=key, default=default)

    def concat(
        self,
        key: str,
        other: pd.DataFrame,
        *,
        ignore_index: bool = False,
        verify_integrity: bool = False,
        sort: bool = False,
    ) -> pd.DataFrame:
        try:
            df = self.get(key=key, ignore_not_found=False)
        except FileNotFoundError:
            new_df = other
        else:
            new_df = pd.concat(
                [df, other],
                ignore_index=ignore_index,
                verify_integrity=verify_integrity,
                sort=sort,
            )
        self.put(key=key, value=new_df)
        return new_df

    def add(self, key: str, entry: Any) -> pd.DataFrame:
        return self.concat(
            key=key, other=pd.DataFrame(entry, index=[0]), ignore_index=True
        )

    def clear(self, key: str):
        """Empty the data at a certain key, leaving you with an empty dataframe on the next .get"""
        return self.put(key=key, value=pd.DataFrame(data=None).reset_index())


class StorageClient:
    def __init__(self):
        # How should we deal with dependency injection in general?
        self._raw_storage = RawStorage()

        self._binary = None
        self._text = None
        self._json = None
        self._dataframes = None

    @property
    def binary(self):
        """Store raw bytes."""
        if self._binary is None:
            self._binary = BinaryStorage(raw_storage=self._raw_storage)
        return self._binary

    @property
    def text(self):
        """Store plain text."""
        if self._text is None:
            self._text = TextStorage(raw_storage=self._raw_storage)
        return self._text

    @property
    def json(self):
        """Store basic python dicts as json."""
        if self._json is None:
            self._json = JsonStorage(raw_storage=self._raw_storage)
        return self._json

    @property
    def dataframes(self):
        """Store and retrieve pandas DataFrames."""
        if self._dataframes is None:
            self._dataframes = DataFramesStorage(raw_storage=self._raw_storage)
        return self._dataframes
