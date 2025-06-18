import os
import json
import logging
import sys
import importlib
from pathlib import Path
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Ensure protobuf-generated modules can be pickled / un-pickled.
# The protobuf code inside ``protocol/files_pb2.py`` declares its classes in
# the *top-level* module ``files_pb2`` (see DESCRIPTOR definition).  When we
# import it via ``protocol.files_pb2`` Python registers the module under the
# key ``protocol.files_pb2`` – but *not* under ``files_pb2``.  ``pickle`` uses
# ``cls.__module__`` (``files_pb2``) when serialising, therefore deserialising
# later would fail unless the alias exists.  The snippet below makes sure the
# alias is present exactly once.
# ---------------------------------------------------------------------------

if 'files_pb2' not in sys.modules:
    try:
        sys.modules['files_pb2'] = importlib.import_module('protocol.files_pb2')
    except ModuleNotFoundError:
        # Not fatal – happens in components that never deal with proto objects.
        pass


class StatePersistence:
    """Generic helper to persist and restore a python object to the filesystem.

    The implementation is intentionally simple so that it can be reused by any
    component that needs a *very* lightweight durability mechanism.  The class
    stores the data in the directory given (defaults to ``/backup`` – the same
    directory already used by other nodes) using **atomic** replacement so that
    corrupted partial writes are avoided.

    By default the data is serialized as JSON, which is enough for basic types
    (``dict``, ``list``, ``int``, ``str`` …).  When you need to persist more
    complex python objects you can choose the *pickle* serializer.
    """

    _JSON = "json"
    _PICKLE = "pickle"
    _SUPPORTED_SERIALIZERS = {_JSON, _PICKLE}

    def __init__(
        self,
        filename: str,
        *,
        directory: str = "/backup",
        serializer: str = _JSON,
    ) -> None:
        if serializer not in self._SUPPORTED_SERIALIZERS:
            raise ValueError(
                f"Unsupported serializer '{serializer}'. "
                f"Choose one of {self._SUPPORTED_SERIALIZERS}."
            )

        self._dir = directory
        # Ensure the target directory exists – it may be mounted as a Docker
        # volume but not created yet.
        os.makedirs(self._dir, exist_ok=True)

        self._file_name = filename
        self._file_path = os.path.join(self._dir, self._file_name)
        self._tmp_path = os.path.join(self._dir, f"temp_{self._file_name}")
        self._serializer = serializer

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def save(self, data: Any) -> None:
        """Persist *data* to disk in an *atomic* fashion."""
        try:
            if self._serializer == self._PICKLE:
                import pickle

                with open(self._tmp_path, "wb") as fp:
                    pickle.dump(data, fp)
            else:
                with open(self._tmp_path, "w") as fp:
                    json.dump(data, fp)
                    fp.flush()

            # Atomic replace – guarantees that either the *old* or the *new*
            # file is present even in case of a crash in between.
            os.replace(self._tmp_path, self._file_path)
        except Exception as exc:
            logging.error(f"[StatePersistence] Error saving state: {exc}")

    def load(self, default_factory: Optional[Callable[[], Any]] = None) -> Any:
        """Load previously persisted data.

        If the file does not exist or loading fails, the *default_factory* is
        invoked (or an empty ``dict`` is returned if *default_factory* is
        ``None``).
        """
        if default_factory is None:
            default_factory = dict  # type: ignore

        if not Path(self._file_path).exists():
            return default_factory()

        try:
            if self._serializer == self._PICKLE:
                import pickle

                with open(self._file_path, "rb") as fp:
                    return pickle.load(fp)
            else:
                with open(self._file_path, "r") as fp:
                    return json.load(fp)
        except Exception as exc:
            logging.error(f"[StatePersistence] Error loading state: {exc}")
            return default_factory()

    def clear(self) -> None:
        """Remove the persisted file from disk if it exists."""
        try:
            if Path(self._file_path).exists():
                os.remove(self._file_path)
        except Exception as exc:
            logging.error(f"[StatePersistence] Error clearing state: {exc}") 