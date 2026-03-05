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
[Bronze Layer] → datos crudos tal como llegan
      ↓
[Silver Layer] → limpieza, tipos correctos, nulos tratados
      ↓
[Gold Layer]   → agregaciones y métricas de negocio
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
│   ├── raw/               # Bronze: datos originales (no versionados)
│   ├── processed/         # Silver: datos limpios
│   └── aggregated/        # Gold: métricas finales
│
├── src/
│   ├── __init__.py
│   ├── ingestion/         # Scripts de carga (Bronze loader)
│   ├── etl/               # Jobs PySpark de transformación
│   └── sql/               # Queries Spark SQL analíticas
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

| Archivo | Descripción | Tamaño aprox |
|---|---|---|
| `train_transaction.csv` | Transacciones con importes, tarjetas y features | ~900 MB |
| `train_identity.csv` | Información de dispositivo y red del usuario | ~200 MB |

### Estadísticas clave del dataset

| Métrica | Valor |
|---|---|
| Total transacciones | 590.540 |
| Columnas | 394 |
| Importe medio | 135 $ |
| Importe máximo | 31.937 $ |
| % Fraude | 3,5% (20.663 transacciones) |
| % No fraude | 96,5% (569.877 transacciones) |

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
jupyter notebook
```

Abre `notebooks/01_exploracion_inicial.ipynb` para ver el análisis exploratorio del dataset.

---

## 📊 Exploración inicial (Notebook 01)

El primer notebook cubre:

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

**2. Carga del dataset**
```python
df_transactions = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("../data/raw/train_transaction.csv")
```

**3. Análisis de nulos en columnas clave**

| Columna | % Nulos | Interpretación |
|---|---|---|
| TransactionID, TransactionAmt, isFraud | 0% | Columnas core, siempre presentes |
| card4, card6 | 0.27% | Nulos residuales, casi completas |
| P_emaildomain | 15.99% | Email comprador, no siempre recogido |
| R_emaildomain | 76.75% | Email destinatario — la mayoría son pagos a comercios, no transferencias |
| dist1 | 59.65% | Distancia entre direcciones, campo opcional |
| dist2 | 93.63% | Segunda distancia, casi siempre vacía |

**4. Distribución de fraude**

| isFraud | Transacciones | % |
|---|---|---|
| 0 (legítima) | 569.877 | 96,5% |
| 1 (fraude) | 20.663 | 3,5% |

> 💡 **Desbalanceo de clases**: el dataset es muy asimétrico (3.5% fraude). Esto es representativo de la realidad bancaria. Las métricas relevantes no son *accuracy* sino **precision, recall y F1-score**.

---

## 🗺️ Roadmap

- [x] Configuración del entorno (Java 11, PySpark 3.5.1)
- [x] Estructura del proyecto (arquitectura Medallion)
- [x] Carga del dataset en Spark
- [x] Análisis exploratorio inicial (schema, nulos, estadísticas)
- [ ] Bronze Layer — script de ingesta
- [ ] Silver Layer — limpieza y transformaciones ETL
- [ ] Gold Layer — métricas de negocio con Spark SQL
- [ ] Orquestación con Apache Airflow
- [ ] Exportación a Parquet

---

## 👤 Autor

**Ignacio Florido**
[GitHub](https://github.com/iflorido/spark-bigdata)
