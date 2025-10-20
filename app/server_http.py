from __future__ import annotations
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

# MCP (FastMCP)
from mcp.server.fastmcp import FastMCP

# --- Carga de scripts del usuario ---
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import build_layer_10_financial as MOD_FINANCIAL
except Exception:
    MOD_FINANCIAL = None

try:
    import build_layer_20_personal as MOD_PERSONAL
except Exception:
    MOD_PERSONAL = None

# ===========
# MCP server (tools)
# ===========
mcp = FastMCP("homolo-mcp")

@mcp.tool()
def build_layer_10_financial(
    input_path: str,
    output_path: str,
    parent: str,
    sheet: Optional[str] = None,
    sheet_name: str = "Resultados",
    parent_name: str = "Datos que fluyen",
    no_parent_row: bool = False,
    pad: int = 2,
) -> Dict[str, Any]:
    if MOD_FINANCIAL is None:
        raise RuntimeError("Módulo build_layer_10_financial no disponible")
    df_raw = MOD_FINANCIAL.read_table(input_path, sheet)
    df_norm = MOD_FINANCIAL.normalize_columns(df_raw)
    df_out = MOD_FINANCIAL.build_layer(
        df_norm,
        parent=parent,
        pad=pad,
        include_parent_row=(not no_parent_row),
        parent_name=parent_name,
    )
    with pd.ExcelWriter(output_path, engine="openpyxl") as xls:
        df_out.to_excel(xls, index=False, sheet_name=sheet_name)
    return {"ok": True, "output": output_path, "rows": int(len(df_out))}

@mcp.tool()
def build_layer_20_personal(
    roles_path: str,
    empleados_path: str,
    output_path: str,
    roles_sheet: Optional[str] = None,
    empleados_sheet: Optional[str] = None,
) -> Dict[str, Any]:
    if MOD_PERSONAL is None:
        raise RuntimeError("Módulo build_layer_20_personal no disponible")
    roles = MOD_PERSONAL.load_roles(roles_path, roles_sheet)
    emps  = MOD_PERSONAL.load_emps(empleados_path, empleados_sheet)
    out   = MOD_PERSONAL.build_layer(roles, emps, parent_code="20")
    with pd.ExcelWriter(output_path, engine="openpyxl") as xls:
        out.to_excel(xls, index=False, sheet_name="20 Personal")
    return {"ok": True, "output": output_path, "rows": int(len(out))}

@mcp.tool()
def file_list(dir_path: str = ".") -> Dict[str, Any]:
    p = Path(dir_path)
    if not p.is_absolute():
        p = (BASE_DIR / dir_path).resolve()
    else:
        p = p.resolve()
    if not p.exists() or not p.is_dir():
        return {"directory": str(p), "items": [], "note": "Directorio no existe o no es carpeta"}
    items: List[Dict[str, Any]] = []
    for child in p.iterdir():
        try:
            items.append({
                "name": child.name,
                "path": str(child),
                "is_dir": child.is_dir(),
                "size": (child.stat().st_size if child.is_file() else None),
            })
        except Exception as e:
            items.append({
                "name": child.name,
                "path": str(child),
                "error": str(e),
            })
    return {"directory": str(p), "items": items}

if __name__ == "__main__":
    # Arranca el servidor HTTP nativo del MCP
    # NOTA: en algunas versiones el nombre del transporte es "http" y en otras "streamable_http".
    host = "0.0.0.0"
    port = int(os.getenv("PORT", 8080))
    token = os.getenv("MCP_BEARER_TOKEN", "")

    # Intenta API moderna (transport="http"); si falla, usa "streamable_http"
    try:
        mcp.run(transport="http", host=host, port=port, bearer_token=token or None)
    except TypeError:
        # versiones previas usan otro parámetro/valor
        mcp.run(transport="streamable_http", host=host, port=port, bearer_token=token or None)
