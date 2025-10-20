
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera la capa 20 = Personal con jerarquía Roles → Empleados.
- Soporta emparejar por NOMBRE de rol o por CÓDIGO de rol.
  * Roles: columns -> code, role
  * Empleados: id, Name (o name), Cost (o cost), role (nombre) y/o cargo (código)
- Símbolo de cabecera por rol:   20.<code>
- Símbolo de empleado:           20.<code>.<id>
"""

from __future__ import annotations
import pandas as pd
import argparse, os, sys, re
from typing import Optional

def read_any(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
        return pd.read_excel(path, sheet_name=(0 if sheet is None else sheet))
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, sep=";")

def norm_cols(df: pd.DataFrame) -> list[str]:
    return [re.sub(r'\s+', ' ', c.strip().lower()) for c in df.columns]

def load_roles(path: str, sheet: Optional[str]) -> pd.DataFrame:
    df = read_any(path, sheet)
    df.columns = norm_cols(df)
    # renombres comunes
    df = df.rename(columns={
        "codigo":"code",
        "código":"code",
        "rol":"role",
        "nombre rol":"role",
    })
    for col in ("code","role"):
        if col not in df.columns:
            raise ValueError(f"[roles] falta columna requerida: {col}. Columnas: {list(df.columns)}")
    df["code"] = df["code"].astype(str).str.strip()
    df["role"] = df["role"].astype(str).str.strip()
    df["_role_key"] = df["role"].str.lower()
    df["_code_key"] = df["code"].str.replace(r"[^0-9A-Za-z]", "", regex=True)
    return df[["code","role","_role_key","_code_key"]]

def load_emps(path: str, sheet: Optional[str]) -> pd.DataFrame:
    df = read_any(path, sheet)
    df.columns = norm_cols(df)
    df = df.rename(columns={
        "nombre":"name",
        "empleado":"name",
        "costo":"cost",
        "valor":"cost",
        "salario":"cost",
    })
    # preferir 'name'/'cost'
    if "name" not in df.columns and "name" in [c.capitalize() for c in df.columns]:
        df = df.rename(columns={"name":"name"})
    if "cost" not in df.columns and "cost" in [c.capitalize() for c in df.columns]:
        df = df.rename(columns={"cost":"cost"})
    # rol por nombre o código
    # admitimos 'role' (texto) y 'cargo' (código)
    if "role" not in df.columns and "rol" in df.columns:
        df = df.rename(columns={"rol":"role"})
    cols_needed = ["id","name","cost"]
    for c in cols_needed:
        if c not in df.columns:
            raise ValueError(f"[empleados] falta columna requerida: {c}. Columnas: {list(df.columns)}")
    # claves de join
    df["_role_key"] = df.get("role", pd.Series([""]*len(df))).astype(str).str.strip().str.lower()
    # code key: de 'cargo' si existe; si no, vacío
    cargo_col = "cargo" if "cargo" in df.columns else None
    if cargo_col:
        df["_code_key"] = df[cargo_col].astype(str).str.strip().str.replace(r"[^0-9A-Za-z]", "", regex=True)
    else:
        df["_code_key"] = ""
    return df

def zpad_keeplen(code: str, min_width: int = 2) -> str:
    # si es completamente numérico, mantén longitud original o al menos min_width
    s = str(code).strip()
    if re.fullmatch(r"\d+", s):
        return s.zfill(max(min_width, len(s)))
    return s

def build_layer(df_roles: pd.DataFrame, df_emp: pd.DataFrame, parent_code: str = "20") -> pd.DataFrame:
    out = []

    # estrategia de mapeo: 1) por _code_key si empleados lo tienen; 2) si no, por _role_key
    emp_has_code = (df_emp["_code_key"] != "").any()

    # ordenar roles por code numérico cuando aplicable
    def sort_key(x):
        try:
            return int(str(x))
        except Exception:
            return x
    roles_sorted = df_roles.sort_values(by="code", key=lambda s: s.map(sort_key))

    for _, r in roles_sorted.iterrows():
        code = r["code"]
        code_key = r["_code_key"]
        role_key = r["_role_key"]
        role_name = r["role"]

        role_symbol = f"{parent_code}.{code}"
        out.append({"parent": parent_code, "symbol": role_symbol, "name": role_name, "input_cost": ""})

        if emp_has_code:
            emps = df_emp[df_emp["_code_key"] == code_key]
        else:
            emps = df_emp[df_emp["_role_key"] == role_key]

        for _, e in emps.iterrows():
            out.append({
                "parent": role_symbol,
                "symbol": f"{role_symbol}.{str(e['id']).strip()}",
                "name": str(e["name"]).strip(),
                "input_cost": e["cost"],
            })

    return pd.DataFrame(out, columns=["parent","symbol","name","input_cost"])

def main():
    ap = argparse.ArgumentParser(description="Capa 20 = Personal (Roles → Empleados)")
    ap.add_argument("--roles", required=True)
    ap.add_argument("--empleados", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--roles-sheet")
    ap.add_argument("--empleados-sheet")
    args = ap.parse_args()

    roles = load_roles(args.roles, args.roles_sheet)
    emps  = load_emps(args.empleados, args.empleados_sheet)
    out = build_layer(roles, emps, parent_code="20")
    with pd.ExcelWriter(args.output, engine="openpyxl") as xls:
        out.to_excel(xls, index=False, sheet_name="20 Personal")

    print(f"OK -> {args.output}")

if __name__ == "__main__":
    main()
