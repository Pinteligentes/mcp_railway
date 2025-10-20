from __future__ import annotations
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

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
# Seguridad (Bearer sencillo)
# ===========
REQUIRED_TOKEN = os.getenv("MCP_BEARER_TOKEN")  # define en Railway

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Permite health-check sin Auth
        if request.url.path in ("/health", "/"):
            return await call_next(request)
        # Protege el endpoint MCP y cualquier otra ruta
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
    Genera la capa 10 (financiera). Requisitos de columnas: code, description, value.
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
    return {"ok": True, "output": output_path, "rows": int(len(df_out))}

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
    return {"ok": True, "output": output_path, "rows": int(len(out))}

# ===========
# Utilidad: listar archivos como TOOL (no resource)
# ===========
@mcp.tool()
def file_list(dir_path: str = ".") -> Dict[str, Any]:
    """
    Lista archivos del directorio indicado (relativo a /app/app o al cwd del contenedor).
    Ejemplo: file_list(dir_path="/data") si montaste un Volume en /data.
    """
    p = (BASE_DIR / dir_path).resolve() if not Path(dir_path).is_absolute() else Path(dir_path).resolve()
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
#
# ===========
# ASGI app y rutas (compatibilidad de versiones)
# ===========
from fastapi import FastAPI

# Intenta la API nueva; si no existe, usa la compat streamable_http_app()
mcp_app = None
try:
    # API más nueva (algunas versiones recientes)
    mcp_app = mcp.http_app(path="/mcp")  # podría no existir en tu versión
    app = FastAPI(title="homolo-mcp", lifespan=mcp_app.lifespan)
    MOUNT_PATH = "/mcp"
except AttributeError:
    # API compatible disponible en mcp 1.12.x+
    if hasattr(mcp, "streamable_http_app"):
        mcp_app = mcp.streamable_http_app()
        # En este modo no se pasa 'path' al crear la app; se monta en FastAPI:
        app = FastAPI(title="homolo-mcp", lifespan=mcp_app.lifespan)
        MOUNT_PATH = "/mcp"
    else:
        raise RuntimeError(
            "Tu versión de mcp no soporta ni http_app() ni streamable_http_app(). "
            "Actualiza a mcp[fastmcp]>=1.12.0."
        )

# Middleware de Bearer que ya definiste:
app.add_middleware(BearerAuthMiddleware)

@app.get("/health")
def health():
    return {"status": "ok"}

# Monta el endpoint MCP en /mcp
app.mount(MOUNT_PATH, mcp_app)