from __future__ import annotations
"""Protocol package initialisation.

Provides *lazy* access to generated protobuf modules so that processes that do
not need them (e.g. the health-checker) are not forced to import `google.protobuf`.

The first time someone evaluates ``protocol.files_pb2`` or ``protocol.comm_pb2``
we dynamically import the generated module and cache it as an attribute of the
``protocol`` package.  This preserves the original `from protocol import
files_pb2` usage pattern while avoiding eager side-effects.
"""

import importlib
import sys
from types import ModuleType
from typing import Any

_GENERATED = {"files_pb2", "comm_pb2"}


def __getattr__(name: str) -> Any:  # noqa: D401 – public hook
    if name in _GENERATED:
        full = f"protocol.{name}"
        module = importlib.import_module(full)
        # Cache the loaded module on the package so subsequent attribute
        # access is O(1) and to keep `is` identity semantics.
        setattr(sys.modules[__name__], name, module)
        return module
    raise AttributeError(name)


# Keep __all__ for * import (unlikely used but good hygiene)
__all__: list[str] = list(_GENERATED) 