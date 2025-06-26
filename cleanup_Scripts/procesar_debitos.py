#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import glob
import os
import shutil

export_path = '../data/Cleaned_data/'
raw_path = '../data/Raw_data/'
archived_path = '../data/Archived_data/'

def procesar_archivo(filepath):
    df = pd.read_csv(
        filepath,
        sep=',',
        header=None,
        encoding='latin-1'
    )

    # 1) Extraer metadata de las 8 primeras columnas (fila 0 = etiquetas, fila 1 = valores)
    labels = df.iloc[0, :8].astype(str).str.strip()
    values = df.iloc[1, :8].astype(str).str.strip()
    metadata_df = pd.DataFrame([values.values], columns=labels.values)
    metadata_df.columns = metadata_df.columns.str.strip()

    # 2) Encontrar el número de cuenta: columna que contenga 'producto'
    prod_cols = [c for c in metadata_df.columns if 'producto' in c.lower()]
    if not prod_cols:
        raise KeyError("No encontré ninguna columna con 'Producto' en metadata")
    account = metadata_df.at[0, prod_cols[0]]

    # 3) Localizar fila de encabezado de transacciones
    hdr_idx = df.index[
        df.iloc[:, 0].astype(str)
          .str.contains('Fecha de Transacción', na=False)
    ][0]
    headers = df.iloc[hdr_idx, :7].astype(str).str.strip().tolist()

    df_tr = df.iloc[hdr_idx + 1:, :7].copy()
    df_tr.columns = headers

    # 4) Limpiar transacciones
    df_tr['Fecha de Transacción'] = pd.to_datetime(
        df_tr['Fecha de Transacción'],
        dayfirst=True,
        errors='coerce'
    )
    df_tr = df_tr.dropna(subset=['Fecha de Transacción'])

    for col in ['Débito de Transacción', 'Crédito de Transacción', 'Balance de Transacción']:
        if col in df_tr.columns:
            df_tr[col] = pd.to_numeric(df_tr[col], errors='coerce')

    # 5) Mes y año del primer registro de transacción
    primera = df_tr['Fecha de Transacción'].iloc[0]
    mes = primera.month
    año = primera.year

    meta_fname = f"Metadata_Debito_{account}_{mes}_{año}.csv"
    tr_fname = f"Transacciones_Debito_{account}_{mes}_{año}.csv"

    metadata_df.to_csv(os.path.join(export_path, meta_fname), index=False, encoding='utf-8-sig')
    df_tr.to_csv(os.path.join(export_path, tr_fname), index=False, encoding='utf-8-sig')
    
    # 6) Mover archivo original a la carpeta de archivados
    dest_path = os.path.join(archived_path, f"Raw_Debito_{account}_{mes}_{año}.csv")
    shutil.move(filepath, dest_path)

    print(f"Procesado: {os.path.basename(filepath)}")
    print(f"  → Metadata:     {meta_fname}")
    print(f"  → Transacciones: {tr_fname}")
    print(f"  → Movimiento: {os.path.basename(filepath)} → {dest_path}\n")


if __name__ == '__main__':
    raws = [
        f for f in glob.glob(os.path.join(raw_path, "*.csv"))
        if not f.lower().startswith(("metadata_debito_", "transacciones_debito_"))
    ]
    for f in raws:
        procesar_archivo(f)

    print("Todos los archivos han sido limpiados y exportados.")