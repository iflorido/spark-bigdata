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
│   └── aggregated/
│       └── gold/                       # Parquet Gold - métricas de negocio
│           ├── fraud_by_hour/
│           ├── fraud_by_card/
│           ├── fraud_by_product/
│           └── fraud_by_month/
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   └── bronze_loader.py            # Carga CSV → Parquet Bronze
│   ├── etl/
│   │   └── silver_transformer.py       # Transformaciones + join → Silver
│   └── sql/
│       └── gold_metrics.py             # Métricas de negocio con Spark SQL
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

## 🚀 Ejecución del pipeline completo

```bash
# Clonar el repositorio
git clone https://github.com/iflorido/spark-bigdata.git
cd spark-bigdata

# Activar entorno
conda activate modspark

# 1. Bronze: ingestar CSV → Parquet
python src/ingestion/bronze_loader.py

# 2. Silver: limpiar, transformar y enriquecer
python src/etl/silver_transformer.py

# 3. Gold: calcular métricas de negocio
python src/sql/gold_metrics.py

# Exploración interactiva
python -m notebook
```

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

## 🥇 Gold Layer — `src/sql/gold_metrics.py`

**Responsabilidad**: calcular métricas de negocio sobre los datos Silver usando Spark SQL puro.

El DataFrame Silver se registra como vista temporal SQL (`createOrReplaceTempView`) y se lanzan queries analíticas sobre 590k transacciones.

**Ejecución:**
```bash
python src/sql/gold_metrics.py
```

---

### Métrica 1 — Fraude por hora del día

| Hora | Transacciones | Fraudes | % Fraude |
|---|---|---|---|
| 08h | 2.869 | 330 | **11.50%** |
| 07h | 4.287 | 407 | **9.49%** |
| 06h | 6.857 | 558 | **8.14%** |
| 13h | 17.665 | 426 | 2.41% |
| 15h | 32.676 | 795 | 2.43% |

> 💡 El fraude se concentra en las horas de madrugada (6-9h), cuando el tráfico es bajo y los usuarios no monitorizan sus cuentas. Es un patrón real conocido en banca.

---

### Métrica 2 — Fraude por tipo de tarjeta

| Red | Tipo | Transacciones | % Fraude | Importe medio fraude |
|---|---|---|---|---|
| discover | credit | 6.304 | **7.93%** | 354 $ |
| mastercard | credit | 50.772 | **6.92%** | 136 $ |
| visa | credit | 83.732 | **6.81%** | 168 $ |
| visa | debit | 301.023 | 2.55% | 135 $ |
| mastercard | debit | 138.415 | 2.16% | 127 $ |

> 💡 Las tarjetas de crédito tienen el triple de fraude que las de débito. Discover credit lidera con el importe medio de fraude más alto (354$).

---

### Métrica 3 — Fraude por categoría de producto

| Producto | Transacciones | % Fraude | Importe medio |
|---|---|---|---|
| C | 68.519 | **11.69%** | 43 $ |
| S | 11.628 | 5.90% | 60 $ |
| H | 33.024 | 4.77% | 73 $ |
| R | 37.699 | 3.78% | 168 $ |
| W | 439.670 | 2.04% | 153 $ |

> 💡 El producto C (micropagos) tiene el mayor % de fraude pero el importe más bajo — patrón típico de fraude de prueba: los defraudadores hacen pequeñas transacciones para verificar que la tarjeta funciona antes de hacer cargos grandes.

---

### Métrica 4 — Evolución mensual del fraude

| Mes | Transacciones | % Fraude | Volumen total |
|---|---|---|---|
| Enero | 92.585 | 4.00% | 12.6 M$ |
| Febrero | 86.021 | 4.01% | 11.9 M$ |
| Marzo | 101.453 | 3.96% | 14.1 M$ |
| Abril | 83.636 | 3.38% | 11.2 M$ |
| Mayo | 89.349 | 3.48% | 12.2 M$ |
| Diciembre | 137.321 | 2.59% | 17.6 M$ |

> 💡 Diciembre tiene el mayor volumen (campaña navideña) pero menor % de fraude — posiblemente por mayor vigilancia bancaria en esas fechas. Junio solo tiene 175 transacciones: el dataset está truncado en ese mes.

---

## 🗺️ Roadmap

- [x] Configuración del entorno (Java 11, PySpark 3.5.1)
- [x] Estructura del proyecto (arquitectura Medallion)
- [x] Análisis exploratorio inicial (schema, nulos, estadísticas, distribución de fraude)
- [x] Bronze Layer — ingesta CSV → Parquet con metadatos de auditoría
- [x] Silver Layer — limpieza, transformación de fechas y join enriched
- [x] Gold Layer — 4 métricas de negocio con Spark SQL
- [ ] Notebook de visualización de resultados (gráficas)
- [ ] Orquestación con Apache Airflow

---

## 👤 Autor

**Ignacio Florido**
[GitHub](https://github.com/iflorido/spark-bigdata)
