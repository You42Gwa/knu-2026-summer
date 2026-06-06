from __future__ import annotations

import re

import pandas as pd

from datastore.state import _df_namespace

_FORBIDDEN_CODE = re.compile(
    r'\b(import|exec|eval|compile|__import__|__builtins__|'
    r'getattr|setattr|delattr|globals|locals|vars|open|input)\b'
    r'|os\.|sys\.|subprocess\.|shutil\.|pathlib\.',
    re.IGNORECASE,
)

try:
    import numpy as np
    _EXEC_GLOBALS: dict = {"pd": pd, "np": np, "__builtins__": {
        "len": len, "str": str, "int": int, "float": float, "bool": bool,
        "list": list, "dict": dict, "tuple": tuple, "set": set,
        "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
        "sorted": sorted, "enumerate": enumerate, "zip": zip,
        "range": range, "isinstance": isinstance,
        "True": True, "False": False, "None": None,
    }}
except ImportError:
    _EXEC_GLOBALS = {"pd": pd, "__builtins__": {
        "len": len, "str": str, "int": int, "float": float, "bool": bool,
        "list": list, "dict": dict, "tuple": tuple, "set": set,
        "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
        "sorted": sorted, "enumerate": enumerate, "zip": zip,
        "range": range, "isinstance": isinstance,
        "True": True, "False": False, "None": None,
    }}


def _clean_code(raw: str) -> str:
    code = re.sub(r"```(?:python)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()
    # 전각 특수문자(U+FF01-FF60) 및 전각 공백 제거
    code = re.sub(r"[！-｠　]", "", code)
    # LLM이 삽입하는 import / from … import 줄 제거
    code = re.sub(r"^(import|from)\s+\S.*$", "", code, flags=re.MULTILINE).strip()
    return code


def _exec_pandas_code(code: str) -> object:
    if _FORBIDDEN_CODE.search(code):
        raise ValueError("금지된 코드 패턴이 감지되었습니다.")
    namespace = dict(_EXEC_GLOBALS)
    namespace.update(_df_namespace)
    exec(code, namespace)
    return namespace.get("result")
