from __future__ import annotations
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd

# Starlette
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
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

# ===========
# Construye la app MCP Starlette (versión-compatible)
# ===========
try:
    mcp_app: Starlette = mcp.http_app(path="/mcp")  # si tu versión lo soporta
except AttributeError:
    # versiones previas: sin argumento 'path'
    mcp_app: Starlette = mcp.streamable_http_app()  # type: ignore

# ===========
# App contenedora que monta MCP en /mcp y añade health + passthrough /
# ===========
container = Starlette(debug=False)

# --- Auth (Bearer) middleware aplicado a TODA la app contenedora ---
REQUIRED_TOKEN = os.getenv("MCP_BEARER_TOKEN", "").strip()

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Permitir health sin auth
        if request.url.path == "/health":
            return await call_next(request)
        # Permitir preflight
        if request.method in ("OPTIONS", "HEAD"):
            return await call_next(request)
        if REQUIRED_TOKEN:
            auth = request.headers.get("authorization", "")
            if not auth.startswith("Bearer "):
                return JSONResponse({"detail": "Missing Bearer token"}, status_code=401)
            token = auth.split(" ", 1)[1].strip()
            if token != REQUIRED_TOKEN:
                return JSONResponse({"detail": "Invalid Bearer token"}, status_code=403)
        return await call_next(request)

container.add_middleware(BearerAuthMiddleware)

@container.route("/health")
async def health(_request: Request):
    return JSONResponse({"status": "ok"})

# Passthrough raíz → /mcp (sin redirección; preserva headers y método)
@container.route("/", methods=["GET", "POST", "OPTIONS", "HEAD"])
async def root_passthrough(request: Request):
    scope = request.scope.copy()
    # si el cliente usa '/', lo reenviamos internamente a '/mcp'
    scope["path"] = "/mcp"
    await mcp_app(scope, request.receive, request._send)
    # respuesta placeholder (no se usa porque mcp_app ya envió respuesta)
    return Response(status_code=204)

# Monta la app MCP en /mcp (también funcionará si el cliente llama explícitamente /mcp)
container.mount("/mcp", mcp_app)

# app exportada para uvicorn
app = container
