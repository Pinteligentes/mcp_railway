from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

# MCP (FastMCP)
from mcp.server.fastmcp import FastMCP

# --- Carga de scripts del usuario ---
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Importa módulos (si fallan, más abajo podemos agregar fallback subprocess)
try:
    import build_layer_10_financial as MOD_FINANCIAL
except Exception:
    MOD_FINANCIAL = None

try:
    import build_layer_20_personal as MOD_PERSONAL
except Exception:
    MOD_PERSONAL = None

# ===========
# Seguridad (Bearer sencillo)
# ===========
REQUIRED_TOKEN = os.getenv("MCP_BEARER_TOKEN")  # define en Railway

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Permite health-check sin Auth
        if request.url.path in ("/health", "/"):
            return await call_next(request)
        # MCP via HTTP usa requests a /mcp/*
        if REQUIRED_TOKEN:
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                raise HTTPException(status_code=401, detail="Missing Bearer token")
            token = auth.split(" ", 1)[1].strip()
            if token != REQUIRED_TOKEN:
                raise HTTPException(status_code=403, detail="Invalid Bearer token")
        return await call_next(request)

# ===========
# MCP server
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
    """
    Genera la capa financiera. Requiere columnas: code, description, value.
    """
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
    return {"ok": True, "output": output_path, "rows": len(df_out)}

@mcp.tool()
def build_layer_20_personal(
    roles_path: str,
    empleados_path: str,
    output_path: str,
    roles_sheet: Optional[str] = None,
    empleados_sheet: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Genera la capa 20 = Personal (Roles → Empleados).
    """
    if MOD_PERSONAL is None:
        raise RuntimeError("Módulo build_layer_20_personal no disponible")
    roles = MOD_PERSONAL.load_roles(roles_path, roles_sheet)
    emps  = MOD_PERSONAL.load_emps(empleados_path, empleados_sheet)
    out   = MOD_PERSONAL.build_layer(roles, emps, parent_code="20")
    with pd.ExcelWriter(output_path, engine="openpyxl") as xls:
        out.to_excel(xls, index=False, sheet_name="20 Personal")
    return {"ok": True, "output": output_path, "rows": len(out)}

# Recurso opcional para listar archivos (útil en remoto)
@mcp.resource("file.list")
def list_files(dir_path: str = ".") -> Dict[str, Any]:
    p = (BASE_DIR / dir_path).resolve()
    items = []
    for child in p.glob("*"):
        items.append({"name": child.name, "is_dir": child.is_dir(),
                      "size": (child.stat().st_size if child.is_file() else None)})
    return {"directory": str(p), "items": items}

# ===========
# ASGI app y rutas
# ===========
mcp_app = mcp.http_app(path="/mcp")
app = FastAPI(title="homolo-mcp")
app.add_middleware(BearerAuthMiddleware)

@app.get("/health")
def health():
    return {"status": "ok"}

# Monta el endpoint MCP en /mcp
app.mount("/mcp", mcp_app)
