#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Construye una “capa” contable a partir de una tabla base.

Entrada esperada (CSV o Excel):
  code, description, value
  43,   Detalle 1,  Valor 1
  44,   Detalle 2,  Valor 2
  ...

Salida (Excel):
  parent, symbol,      name,       input_cost
  10.01, 10.01,        <parent_name>, 
  10.01, 10.01.43,     Detalle 1,  Valor 1
  10.01, 10.01.44,     Detalle 2,  Valor 2
  ...

Uso:
  python build_financial_layer.py --input entrada.xlsx --output salida.xlsx \
         --parent 10.01 --parent-name "Resultados financieros" --sheet-name "10 Resultados financieros"
"""

import argparse
import os
import sys
import pandas as pd
from typing import Optional


def read_table(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xls", ".xlsx", ".xlsm", ".xlsb"]:
        return pd.read_excel(path, sheet_name=sheet)
    elif ext in [".csv", ".txt"]:
        # intenta coma; si falla, punto y coma
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.read_csv(path, sep=";")
    else:
        raise ValueError(f"Formato no soportado: {ext}")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza nombres
    rename_map = {c.lower().strip(): c for c in df.columns}
    # Mapea variaciones comunes a oficiales
    aliases = {
        "codigo": "code",
        "code": "code",
        "descripcion": "description",
        "description": "description",
        "valor": "value",
        "value": "value",
        "input_cost": "value",
        "importe": "value",
        "monto": "value",
    }
    # Construye nuevo dict de renombres
    newcols = {}
    for c in df.columns:
        key = c.lower().strip()
        if key in aliases:
            newcols[c] = aliases[key]
    df = df.rename(columns=newcols)
    required = ["code", "description", "value"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Faltan columnas requeridas: {missing}. Presentes: {list(df.columns)}")
    return df[required].copy()


def build_layer(df_in: pd.DataFrame, parent: str, pad: int = 2,
                include_parent_row: bool = True, parent_name: Optional[str] = None) -> pd.DataFrame:
    out_rows = []
    if include_parent_row:
        out_rows.append(
            {
                "parent": parent,
                "symbol": parent,
                "name": parent_name if parent_name is not None else "",
                "input_cost": ""
            }
        )
    # limpia y arma símbolos
    for _, row in df_in.iterrows():
        code = str(row["code"]).strip()
        # intento forzar a entero para pad, pero si trae letras se respeta
        try:
            code_int = int(float(code))
            code_str = str(code_int).zfill(pad)
        except Exception:
            # si no es número, no se rellena con ceros
            code_str = code
        symbol = f"{parent}.{code_str}"
        out_rows.append(
            {
                "parent": parent,
                "symbol": symbol,
                "name": str(row["description"]).strip(),
                "input_cost": row["value"],
            }
        )
    return pd.DataFrame(out_rows, columns=["parent", "symbol", "name", "input_cost"])


def main():
    ap = argparse.ArgumentParser(
        description="Genera una capa contable estilo 'parent/symbol/name/input_cost' desde una tabla base.")
    ap.add_argument("--input", "-i", required=True,
                    help="Ruta del archivo de entrada (CSV/XLSX).")
    ap.add_argument("--sheet", help="Nombre de hoja si el input es Excel.")
    ap.add_argument("--output", "-o", required=True,
                    help="Ruta del Excel de salida (e.g., salida.xlsx).")
    ap.add_argument("--sheet-name", default="Resultados",
                    help="Nombre de hoja en el Excel de salida.")
    ap.add_argument("--parent", required=True,
                    help="Código padre (ej: 10.01).")
    ap.add_argument("--parent-name", default="Datos que fluyen",
                    help="Nombre del nodo padre (fila 1).")
    ap.add_argument("--no-parent-row", action="store_true",
                    help="No incluir la fila del padre.")
    ap.add_argument("--pad", type=int, default=2,
                    help="Relleno con ceros para el code (por defecto 2 -> 43 -> '43').")
    args = ap.parse_args()

    try:
        df_raw = read_table(args.input, args.sheet)
        df_norm = normalize_columns(df_raw)
        df_out = build_layer(
            df_norm,
            parent=args.parent,
            pad=args.pad,
            include_parent_row=not args.no_parent_row,
            parent_name=args.parent_name
        )
        # exporta a Excel
        with pd.ExcelWriter(args.output, engine="openpyxl") as xls:
            df_out.to_excel(xls, index=False, sheet_name=args.sheet_name)
        print(f"✅ Archivo generado: {args.output} (hoja: {args.sheet_name})")
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
