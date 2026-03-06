# 🏦 Financial Transactions Data Pipeline

Pipeline ETL de análisis y detección de patrones en transacciones bancarias usando **PySpark**, **Spark SQL** y arquitectura **Medallion (Bronze/Silver/Gold)**.

---

## 📋 Descripción del proyecto

Este proyecto simula el trabajo real de un ingeniero de datos en el sector financiero: ingestar millones de transacciones bancarias, procesarlas con Apache Spark y dejarlas listas para análisis de negocio.

El dataset utilizado es el **IEEE-CIS Fraud Detection** (Kaggle), con más de 590.000 transacciones reales anonimizadas.

---

## 🛠️ Stack tecnológico

| Capa | Herramienta | Versión |
|---|---|---|
| Lenguaje | Python | 3.x |
| Big Data | PySpark | 3.5.1 |
| SQL | Spark SQL | 3.5.1 |
| Almacenamiento | Parquet (columnar) | - |
| Entorno | Conda | modspark |
| Runtime | Java (OpenJDK) | 11 |
| Notebooks | Jupyter | - |
| Control de versiones | Git + GitHub | - |

---

## 🏗️ Arquitectura Medallion

```
[Datos raw CSV]
      ↓
[Bronze Layer] → datos crudos tal como llegan + metadatos de auditoría
      ↓
[Silver Layer] → limpieza, tipos correctos, nulos tratados, join enriched
      ↓
[Gold Layer]   → agregaciones y métricas de negocio con Spark SQL
      ↓
[Análisis / Dashboard]
```

La arquitectura **Medallion** es el estándar en plataformas de datos modernas (Databricks, Azure, BBVA, Santander). Cada capa tiene una responsabilidad clara y los datos solo avanzan hacia adelante.

---

## 📁 Estructura del repositorio

```
spark-bigdata/
│
├── data/
│   ├── raw/                            # Datos originales CSV (no versionados)
│   ├── processed/
│   │   ├── bronze/
│   │   │   ├── transactions/           # Parquet Bronze - transacciones
│   │   │   └── identity/               # Parquet Bronze - identidad
│   │   └── silver/
│   │       └── transactions_enriched/  # Parquet Silver - join enriquecido
│   └── aggregated/                     # Gold: métricas finales (pendiente)
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   └── bronze_loader.py            # Carga CSV → Parquet Bronze
│   ├── etl/
│   │   └── silver_transformer.py       # Transformaciones + join → Silver
│   └── sql/                            # Queries Spark SQL analíticas (pendiente)
│
├── notebooks/
│   └── 01_exploracion_inicial.ipynb
│
├── .gitignore
├── requirements.txt
└── README.md
```

> ⚠️ La carpeta `data/` está excluida del repositorio via `.gitignore` por tamaño y privacidad de los datos.

---

## 📦 Dataset

**IEEE-CIS Fraud Detection** — [Kaggle](https://www.kaggle.com/c/ieee-fraud-detection)

| Archivo | Descripción | Tamaño |
|---|---|---|
| `train_transaction.csv` | Transacciones con importes, tarjetas y features | 652 MB |
| `train_identity.csv` | Información de dispositivo y red del usuario | 25 MB |

### Estadísticas clave del dataset

| Métrica | Valor |
|---|---|
| Total transacciones | 590.540 |
| Columnas originales | 394 |
| Importe medio | 135 $ |
| Importe máximo | 31.937 $ |
| % Fraude | 3,5% (20.663 transacciones) |
| % No fraude | 96,5% (569.877 transacciones) |
| Transacciones con datos de identidad | 24,4% (144.233) |

---

## ⚙️ Instalación y configuración

### 1. Requisitos previos

- **Java 11** (OpenJDK) — requerido por PySpark
- **Conda** — para gestión del entorno

### 2. Verificar Java

```bash
java -version
# Debe mostrar openjdk version "11.x.x"
```

> ⚠️ PySpark 3.5.x requiere Java 11. Las versiones 17+ o 18+ pueden causar errores inesperados en la JVM.

En macOS, instalar con Homebrew y configurar en el entorno conda:

```bash
brew install openjdk@11
conda activate modspark
conda env config vars set JAVA_HOME=$(/usr/libexec/java_home -v 11)
conda deactivate && conda activate modspark
```

### 3. Crear entorno conda

```bash
conda create -n modspark python=3.10
conda activate modspark
```

### 4. Instalar dependencias

```bash
pip install pyspark==3.5.1
pip install pandas
pip install faker
pip install jupyter
```

### 5. Verificar instalación

```bash
python -c "import pyspark; print(pyspark.__version__)"
# Debe mostrar: 3.5.1
```

---

## 🚀 Inicio rápido

```bash
# Clonar el repositorio
git clone https://github.com/iflorido/spark-bigdata.git
cd spark-bigdata

# Activar entorno
conda activate modspark

# Arrancar Jupyter
python -m notebook     # Usa la instalación del entorno conda
```

Abre `notebooks/01_exploracion_inicial.ipynb` para ver el análisis exploratorio del dataset.

---

## 📊 Exploración inicial — `notebooks/01_exploracion_inicial.ipynb`

**1. Arranque de SparkSession**
```python
spark = SparkSession.builder \
    .appName("FinancialPipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```
- `local[*]` → usa todos los cores disponibles en modo local
- `spark.driver.memory` → memoria asignada al driver

**2. Análisis de nulos en columnas clave**

| Columna | % Nulos | Interpretación |
|---|---|---|
| TransactionID, TransactionAmt, isFraud | 0% | Columnas core, siempre presentes |
| card4, card6 | 0.27% | Nulos residuales, casi completas |
| P_emaildomain | 15.99% | Email comprador, no siempre recogido |
| R_emaildomain | 76.75% | Email destinatario — mayoría son pagos a comercios, no transferencias |
| dist1 | 59.65% | Distancia entre direcciones, campo opcional |
| dist2 | 93.63% | Segunda distancia, casi siempre vacía |

**3. Distribución de fraude**

| isFraud | Transacciones | % |
|---|---|---|
| 0 (legítima) | 569.877 | 96,5% |
| 1 (fraude) | 20.663 | 3,5% |

> 💡 **Desbalanceo de clases**: el dataset es muy asimétrico (3.5% fraude). Representativo de la realidad bancaria. Las métricas relevantes no son *accuracy* sino **precision, recall y F1-score**.

---

## 🥉 Bronze Layer — `src/ingestion/bronze_loader.py`

**Responsabilidad**: ingestar los CSV originales y guardarlos en Parquet sin modificaciones, añadiendo únicamente metadatos de auditoría.

**Transformaciones aplicadas:**
- Lectura de CSV con inferencia de schema
- Añadir columna `ingestion_timestamp` — cuándo se ingirió el dato
- Añadir columna `source_file` — de qué fichero proviene
- Escritura en formato Parquet con compresión Snappy

**Ejecución:**
```bash
python src/ingestion/bronze_loader.py
```

**Resultado de compresión CSV → Parquet:**

| Tabla | CSV original | Parquet Bronze | Reducción |
|---|---|---|---|
| transactions | 652 MB | 77 MB | **88%** |
| identity | 25 MB | 3.4 MB | **86%** |

> 💡 Parquet es un formato columnar que agrupa valores similares juntos, permitiendo una compresión muy superior al CSV. En producción nadie trabaja con CSVs para Big Data.

---

## 🥈 Silver Layer — `src/etl/silver_transformer.py`

**Responsabilidad**: limpiar, transformar y enriquecer los datos de Bronze para dejarlos listos para análisis.

**Transformaciones aplicadas:**
- `TransactionDT` (entero en segundos) → `transaction_date` (timestamp real desde fecha base 2017-11-30)
- Extracción de `transaction_hour`, `transaction_day`, `transaction_month`
- Tratamiento de nulos con criterio de negocio:
  - `P_emaildomain` → `"unknown"`
  - `R_emaildomain` → `"no_recipient"` (mayoría son pagos a comercios, no transferencias entre personas)
  - `dist1`, `dist2` → `-1.0` (centinela: distinto de 0, que es un valor válido)
- Eliminación de columnas `V1-V339` (features anonimizadas no necesarias en esta capa)
- **Left join** transactions + identity por `TransactionID`
- 394 columnas originales reducidas a 15 columnas de negocio relevantes

**Ejecución:**
```bash
python src/etl/silver_transformer.py
```

**Resultado:**

| Métrica | Valor |
|---|---|
| Parquet Bronze transactions | 77 MB |
| Parquet Silver enriched | 16 MB |
| Reducción adicional | **79%** |
| Transacciones con identidad | 24,4% (144.233) |

> 💡 **¿Por qué LEFT JOIN y no INNER JOIN?** Con INNER JOIN perderíamos las transacciones sin datos de identidad. En fraude, una transacción sin datos de dispositivo puede ser precisamente la más sospechosa.

---

## 🗺️ Roadmap

- [x] Configuración del entorno (Java 11, PySpark 3.5.1)
- [x] Estructura del proyecto (arquitectura Medallion)
- [x] Análisis exploratorio inicial (schema, nulos, estadísticas, distribución de fraude)
- [x] Bronze Layer — ingesta CSV → Parquet con metadatos de auditoría
- [x] Silver Layer — limpieza, transformación de fechas y join enriched
- [ ] Gold Layer — métricas de negocio con Spark SQL
- [ ] Orquestación con Apache Airflow
- [ ] Notebook de visualización de resultados

---

## 👤 Autor

**Ignacio Florido**
[GitHub](https://github.com/iflorido/spark-bigdata)
