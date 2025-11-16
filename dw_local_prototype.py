import os
import pandas as pd
import sqlite3
from datetime import datetime
import logging

# Configuración de logging para rastrear el proceso
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definir paths basados en la estructura de carpetas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Directorio del script
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
STAGING_DIR = os.path.join(DATA_DIR, 'staging')
DW_DIR = os.path.join(DATA_DIR, 'dw')

# Asegurarse de que las carpetas existan
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(DW_DIR, exist_ok=True)

# Meses disponibles (basado en los datos: enero, febrero, marzo 2025)
MONTHS = ['2025-01', '2025-02', '2025-03']
EXCEL_FILES = {
    '2025-01': '01_Exportaciones_2025_Enero.xlsx',
    '2025-02': '02_Exportaciones_2025_Febrero.xlsx',
    '2025-03': '03_Exportaciones_2025_Marzo.xlsx'
}

# Función para leer un archivo Excel y manejar truncamientos
def read_excel_to_staging(month, file_name):
    raw_path = os.path.join(RAW_DIR, month, file_name)
    staging_path = os.path.join(STAGING_DIR, f'staging_{month}.csv')
    
    logging.info(f"Leyendo archivo raw: {raw_path}")
    
    # Leer el Excel (asumiendo Sheet1)
    try:
        df = pd.read_excel(raw_path, sheet_name='Sheet1', engine='openpyxl')
    except FileNotFoundError:
        logging.warning(f"Archivo no encontrado para {month}: {raw_path}. Saltando.")
        return pd.DataFrame()  # Retorna vacío si no existe
    
    # Limpieza mínima en staging: eliminar filas vacías, normalizar columnas
    df = df.dropna(how='all')
    df.columns = df.columns.str.strip().str.upper()
    
    logging.info(f"Datos leídos para {month}: {df.shape[0]} filas")
    
    # Guardar en staging como CSV
    df.to_csv(staging_path, index=False)
    logging.info(f"Guardado en staging: {staging_path}")
    
    return df

# Etapa 1: Ingestión a Staging Layer
def ingest_to_staging():
    staging_dfs = {}
    for month in MONTHS:
        file_name = EXCEL_FILES.get(month)
        if file_name:
            staging_dfs[month] = read_excel_to_staging(month, file_name)
    return staging_dfs

# Etapa 2: Transformación a Core Layer (integración y limpieza)
def transform_to_core(staging_dfs):
    # Unir todos los meses en un solo DataFrame (ignorar vacíos)
    all_data = pd.concat([df for df in staging_dfs.values() if not df.empty], ignore_index=True)
    
    if all_data.empty:
        logging.error("No hay datos en staging. Terminando.")
        return pd.DataFrame()
    
    logging.info(f"Datos integrados en core: {all_data.shape[0]} filas")

    # --- CORRECCIÓN DE NOMBRES DE COLUMNA ---
    # Normalizar nombres: eliminar espacios y mayúsculas
    all_data.columns = all_data.columns.str.strip().str.upper()
    
    # Forzar nombre correcto de NUMERO_SERIE
    if 'NUM_SERIE' in all_data.columns:
        all_data = all_data.rename(columns={'NUM_SERIE': 'NUMERO_SERIE'})
    elif 'NUMERO SERIE' in all_data.columns:
        all_data = all_data.rename(columns={'NUMERO SERIE': 'NUMERO_SERIE'})
    
    # Verificar que existan las columnas clave
    required_cols = ['NUMERO_FORMULARIO', 'NUMERO_SERIE']
    missing = [col for col in required_cols if col not in all_data.columns]
    if missing:
        logging.error(f"Columnas faltantes para deduplicación: {missing}")
        return pd.DataFrame()

    # --- FIN CORRECCIÓN ---

    # Limpieza en core:
    all_data['FECHA_DECLARACION_EXPORTACION'] = pd.to_datetime(
        all_data['FECHA_DECLARACION_EXPORTACION'].astype(str),
        format='%Y%m%d', errors='coerce'
    )
    
    # Convertir numéricas
    numeric_cols = ['CANTIDAD_UNIDADES_FISICAS', 'PESO_BRUTO_KGS', 'PESO_NETO_KGS', 'VALOR_FOB_USD', 'VALOR_FOB_PESOS']
    for col in numeric_cols:
        if col in all_data.columns:
            all_data[col] = pd.to_numeric(all_data[col], errors='coerce')
    
    # Eliminar duplicados
    all_data = all_data.drop_duplicates(subset=['NUMERO_FORMULARIO', 'NUMERO_SERIE'])
    
    # Manejar nulos
    for col in numeric_cols:
        if col in all_data.columns:
            all_data[col] = all_data[col].fillna(0)
    if 'PAIS_DESTINO_FINAL' in all_data.columns:
        all_data['PAIS_DESTINO_FINAL'] = all_data['PAIS_DESTINO_FINAL'].fillna('Unknown')
    
    # Guardar en core como Parquet
    core_path = os.path.join(DW_DIR, 'core_exportaciones.parquet')
    all_data.to_parquet(core_path, engine='pyarrow')
    logging.info(f"Guardado en core: {core_path}")
    
    return all_data

# Etapa 3: Semantic Layer (Modelo Dimensional: Dimensiones y Hechos en SQLite)
def build_semantic_layer(core_df):
    if core_df.empty:
        logging.error("Core DF vacío. No se puede construir semantic layer.")
        return
    
    conn = sqlite3.connect(os.path.join(DW_DIR, 'dw_exportaciones.db'))
    logging.info("Conectado a SQLite para Semantic Layer")
    
    # Dimensión Tiempo
    dim_time = core_df[['FECHA_DECLARACION_EXPORTACION']].drop_duplicates().reset_index(drop=True)
    dim_time['TIME_ID'] = dim_time.index + 1
    dim_time['YEAR'] = dim_time['FECHA_DECLARACION_EXPORTACION'].dt.year
    dim_time['MONTH'] = dim_time['FECHA_DECLARACION_EXPORTACION'].dt.month
    dim_time['DAY'] = dim_time['FECHA_DECLARACION_EXPORTACION'].dt.day
    dim_time.to_sql('DIM_TIME', conn, if_exists='replace', index=False)
    
    # Dimensión Empresa
    dim_empresa = core_df[['NIT_EXPORTADOR', 'RAZON_SOCIAL_EXPORTADOR', 'DIREC_EXPORTADOR']].drop_duplicates().reset_index(drop=True)
    dim_empresa['EMPRESA_ID'] = dim_empresa.index + 1
    dim_empresa.to_sql('DIM_EMPRESA', conn, if_exists='replace', index=False)
    
    # Dimensión Pais Destino
    dim_pais = core_df[['COD_PAIS_DESTINO', 'PAIS_DESTINO_FINAL']].drop_duplicates().reset_index(drop=True)
    dim_pais['PAIS_ID'] = dim_pais.index + 1
    dim_pais.to_sql('DIM_PAIS', conn, if_exists='replace', index=False)
    
    # Dimensión Mercancia
    dim_mercancia = core_df[['SUBPARTIDA']].drop_duplicates().reset_index(drop=True)
    dim_mercancia['MERCANCIA_ID'] = dim_mercancia.index + 1
    dim_mercancia.to_sql('DIM_MERCANCIA', conn, if_exists='replace', index=False)
    
    # Tabla de Hechos
    fact_exportaciones = core_df.merge(dim_time, on='FECHA_DECLARACION_EXPORTACION')
    fact_exportaciones = fact_exportaciones.merge(dim_empresa, on=['NIT_EXPORTADOR', 'RAZON_SOCIAL_EXPORTADOR', 'DIREC_EXPORTADOR'])
    fact_exportaciones = fact_exportaciones.merge(dim_pais, on=['COD_PAIS_DESTINO', 'PAIS_DESTINO_FINAL'])
    fact_exportaciones = fact_exportaciones.merge(dim_mercancia, on='SUBPARTIDA')
    
    fact_exportaciones = fact_exportaciones[[
        'TIME_ID', 'EMPRESA_ID', 'PAIS_ID', 'MERCANCIA_ID',
        'VALOR_FOB_USD', 'PESO_NETO_KGS', 'CANTIDAD_UNIDADES_FISICAS',
        'NUMERO_FORMULARIO'
    ]]
    fact_exportaciones.to_sql('FACT_EXPORTACIONES', conn, if_exists='replace', index=False)
    
    logging.info("Semantic Layer construida en SQLite")
    conn.close()

# Función para ejecutar consultas SQL en el DW
def query_dw(sql_query):
    conn = sqlite3.connect(os.path.join(DW_DIR, 'dw_exportaciones.db'))
    df = pd.read_sql_query(sql_query, conn)
    conn.close()
    return df

# Responder preguntas del cliente y análisis adicionales
def analyze_data():
    # Pregunta 1: Empresas que más exportaron en el último mes (asumiendo marzo como último)
    q1 = """
    SELECT e.RAZON_SOCIAL_EXPORTADOR, SUM(f.VALOR_FOB_USD) as TOTAL_FOB_USD
    FROM FACT_EXPORTACIONES f
    JOIN DIM_TIME t ON f.TIME_ID = t.TIME_ID
    JOIN DIM_EMPRESA e ON f.EMPRESA_ID = e.EMPRESA_ID
    WHERE t.YEAR = 2025 AND t.MONTH = 3
    GROUP BY e.RAZON_SOCIAL_EXPORTADOR
    ORDER BY TOTAL_FOB_USD DESC
    LIMIT 10;
    """
    top_empresas = query_dw(q1)
    print("Top 10 Empresas que más exportaron en el último mes (Marzo 2025):")
    print(top_empresas)
    
    # Pregunta 2: Valor total FOB mes a mes
    q2 = """
    SELECT t.YEAR, t.MONTH, SUM(f.VALOR_FOB_USD) as TOTAL_FOB_USD
    FROM FACT_EXPORTACIONES f
    JOIN DIM_TIME t ON f.TIME_ID = t.TIME_ID
    GROUP BY t.YEAR, t.MONTH
    ORDER BY t.YEAR, t.MONTH;
    """
    total_mes = query_dw(q2)
    print("\nValor Total FOB Mes a Mes:")
    print(total_mes)
    
    # Pregunta 3: Destinos donde más se exporta en los últimos 6 meses (usamos los 3 disponibles)
    q3 = """
    SELECT p.PAIS_DESTINO_FINAL, SUM(f.VALOR_FOB_USD) as TOTAL_FOB_USD
    FROM FACT_EXPORTACIONES f
    JOIN DIM_TIME t ON f.TIME_ID = t.TIME_ID
    JOIN DIM_PAIS p ON f.PAIS_ID = p.PAIS_ID
    WHERE t.YEAR = 2025 AND t.MONTH BETWEEN 1 AND 3
    GROUP BY p.PAIS_DESTINO_FINAL
    ORDER BY TOTAL_FOB_USD DESC
    LIMIT 10;
    """
    top_destinos = query_dw(q3)
    print("\nTop 10 Destinos en los Últimos 3 Meses:")
    print(top_destinos)
    
    # Análisis adicional 1: Top 10 Productos (subpartidas) más exportados por valor FOB
    a1 = """
    SELECT m.SUBPARTIDA, SUM(f.VALOR_FOB_USD) as TOTAL_FOB_USD
    FROM FACT_EXPORTACIONES f
    JOIN DIM_MERCANCIA m ON f.MERCANCIA_ID = m.MERCANCIA_ID
    GROUP BY m.SUBPARTIDA
    ORDER BY TOTAL_FOB_USD DESC
    LIMIT 10;
    """
    top_productos = query_dw(a1)
    print("\nAnálisis Adicional 1: Top 10 Productos Más Exportados por Valor FOB:")
    print(top_productos)
    
    # Análisis adicional 2: Top 10 Países por Peso Neto Exportado (concentración por peso)
    a2 = """
    SELECT p.PAIS_DESTINO_FINAL, SUM(f.PESO_NETO_KGS) as TOTAL_PESO_NETO_KGS
    FROM FACT_EXPORTACIONES f
    JOIN DIM_PAIS p ON f.PAIS_ID = p.PAIS_ID
    GROUP BY p.PAIS_DESTINO_FINAL
    ORDER BY TOTAL_PESO_NETO_KGS DESC
    LIMIT 10;
    """
    concentracion_peso = query_dw(a2)
    print("\nAnálisis Adicional 2: Top 10 Países por Peso Neto Exportado:")
    print(concentracion_peso)

# Flujo principal
if __name__ == "__main__":
    logging.info("Iniciando prototipo de Data Warehouse")
    
    # Ingestión
    staging_dfs = ingest_to_staging()
    
    # Transformación
    core_df = transform_to_core(staging_dfs)
    
    # Semantic
    build_semantic_layer(core_df)
    
    # Análisis
    analyze_data()
    
    logging.info("Proceso completado")