# Final_Ingenieria_De_Datos

# Data Warehouse Local - Exportaciones Colombia (2025)

Un **prototipo completo de Data Warehouse** en Python para analizar exportaciones colombianas (Enero, Febrero y Marzo 2025).

---

## Objetivo

Procesar 3 archivos Excel de exportaciones → **Staging → Core → Modelo Dimensional → Análisis**, todo **localmente** con:
- `pandas` → ETL
- `Parquet` → almacenamiento eficiente
- `SQLite` → capa semántica (dimensiones + hechos)
- Reportes automáticos en consola

---

## Estructura de Carpetas

```
dw_exportaciones/
├── dw_local_prototype.py          ← Script principal (pega aquí el código)
└── data/
    └── raw/
        ├── 2025-01/
        │   └── 01_Exportaciones_2025_Enero.xlsx
        ├── 2025-02/
        │   └── 02_Exportaciones_2025_Febrero.xlsx
        └── 2025-03/
            └── 03_Exportaciones_2025_Marzo.xlsx
```

> Los Excel **deben estar en subcarpetas por mes**.

---

## Requisitos

```bash
pip install pandas openpyxl pyarrow
```

---

## Cómo Ejecutar

```bash
cd dw_exportaciones
python3 dw_local_prototype.py
```

---

## Qué Hace el Script

| Etapa | Acción |
|------|-------|
| **1. Staging** | Lee cada Excel → limpia → guarda como CSV |
| **2. Core** | Une los 3 meses → limpia fechas, números → elimina duplicados → guarda en Parquet |
| **3. Semantic Layer** | Crea modelo dimensional en SQLite (`DIM_*` y `FACT_EXPORTACIONES`) |
| **4. Análisis** | Imprime en consola: |
| | • Top 10 empresas (marzo) |
| | • Valor FOB total por mes |
| | • Top 10 destinos (3 meses) |
| | • Top 10 productos por valor |
| | • Top 10 países por peso |

---

## Salida Generada

```
data/staging/      → staging_2025-01.csv, ...
data/dw/
├── core_exportaciones.parquet
└── dw_exportaciones.db     ← Base de datos SQLite (puedes abrirla con DB Browser)
```

---

## Errores Comunes (y Solución)

| Error | Causa | Solución |
|------|------|---------|
| `KeyError: 'NUM_SERIE'` | Columna mal escrita | El código ya la renombra a `NUMERO_SERIE` |
| `FileNotFoundError` | Excel no está en la carpeta correcta | Verifica `data/raw/2025-0X/` |

---

**Listo en 2 minutos. Local, rápido, sin nube.**

> Hecho por Grok + tú. 2025
