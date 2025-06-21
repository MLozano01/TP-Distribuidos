import os
import json
import logging
import sys
import importlib
from pathlib import Path
from typing import Any, Callable, Optional
import glob


if 'files_pb2' not in sys.modules:
    try:
        sys.modules['files_pb2'] = importlib.import_module('protocol.files_pb2')
    except ModuleNotFoundError:
        pass


class StatePersistence:
    _JSON = "json"
    _PICKLE = "pickle"
    _SUPPORTED_SERIALIZERS = {_JSON, _PICKLE}
    BASE_FILE_NAME = "secuence_numbers_"

    def __init__(self, node_info, filename: str, *, directory: str = "/backup", serializer: str = _JSON) -> None:
        if serializer not in self._SUPPORTED_SERIALIZERS:
            raise ValueError(
                f"Unsupported serializer '{serializer}'. "
                f"Choose one of {self._SUPPORTED_SERIALIZERS}."
            )

        self._dir = directory
        os.makedirs(self._dir, exist_ok=True)

        self.node_info = node_info
        self._file_name = filename
        self._file_path = os.path.join(self._dir, self._file_name)
        self._tmp_path = os.path.join(self._dir, f"temp_{self._file_name}")
        self._serializer = serializer

    def save(self, data: Any) -> None:
        """Persist *data* to disk in an *atomic* fashion."""
        try:
            if self._serializer == self._PICKLE:
                import pickle

                with open(self._tmp_path, "wb") as fp:
                    pickle.dump(data, fp)
                    #fp.flush()
                    #os.fsync(fp.fileno())
            else:
                with open(self._tmp_path, "w") as fp:
                    json.dump(data, fp)
                    fp.flush()
                    #os.fsync(fp.fileno())

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
            default_factory = dict

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

    def save_secuence_number_data(self, info_to_save, client_id):
        tempfile = f"/backup/temp_{self.BASE_FILE_NAME}_{self.node_info}_{client_id}.txt"
        actual_file = f"/backup/{self.BASE_FILE_NAME}_{self.node_info}_client_{client_id}.txt"

        try:
            if os.path.exists(actual_file):
                with open(actual_file, 'r') as f:
                    existing = f.read()
            else:
                existing = ""

            with open(tempfile, 'w') as f:
                f.write(existing)
                f.write(f"{info_to_save}\n")
                f.flush()

            os.replace(tempfile, actual_file)

        except Exception as e:
            logging.error(f"ERROR saving secuence numbe info to file {actual_file}: {e}")

    def load_saved_secuence_number_data(self):
        try:
            backup = {}

            for filepath in glob.glob(f'/backup/{self.BASE_FILE_NAME}_{self.node_info}_*.txt'):
                client = filepath.split("_")[3]
                with open(filepath) as f:
                    backup.setdefault(client, [])
                    for line in f:
                        backup[client].append(line.strip())     
            return backup       
            
        except Exception as e:
            logging.error(f"ERROR reading from file: {e}")

    def clear(self) -> None:
        """Remove the persisted file from disk if it exists."""
        try:
            if Path(self._file_path).exists():
                os.remove(self._file_path)
        except Exception as exc:
            logging.error(f"[StatePersistence] Error clearing state: {exc}") 

    def clean_client(self, client_id) -> None:
        """Remove the file from disk if it exists."""
        path = f"/backup/{self.BASE_FILE_NAME}_{self.node_info}_{client_id}"
        try:
            if Path(path).exists():
                os.remove(path)
        except Exception as exc:
            logging.error(f"[StatePersistence] Error clearing state: {exc}") 