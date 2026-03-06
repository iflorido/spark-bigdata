from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    
    """ Creamos una sesión de Spark con el nombre de la 
    aplicación proporcionado"""
    
    return SparkSession.builder.appName(app_name).master("local[*]").config("spark.driver.memory", "4g").getOrCreate()

def load_to_bronze(spark: SparkSession, input_path: str, output_path: str, table_name: str):
    
    """
    Vamos a leer un CSV y lo guarda en formato Parquet (capa Bronze).
    
    Args:
        spark: SparkSession activa
        input_path: ruta al CSV de origen
        output_path: ruta donde guardar el Parquet
        table_name: nombre descriptivo para los logs
    """
    logger.info(f"Primera parte: cargar datos en la capa Bronze: {table_name}")
    
    # Leemos el CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # Añadimos metadata de carga para rastrear el proceso
    df = df.withColumn("ingestion_timestamp", F.current_timestamp()).withColumn("source_file", F.lit(input_path))
    
    row_count = df.count()
    logger.info(f"{table_name}: {row_count} filas leídas")
    
    # Guardar como Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Datos de la tabla {table_name}: guardado en {output_path} en la capa Bronze")
    return row_count

if __name__ == "__main__":
    
    PROJECT_ROOT = Path(__file__).resolve().parents[2] # Ajusta esta ruta según la estructura de tu proyecto
    spark = create_spark_session("BronzeLoader-App-Finanzas")
    
    # aquí cargamos las transacciones financieras
    load_to_bronze(
        spark=spark,
        input_path=str(PROJECT_ROOT / "data/raw/train_transaction.csv"),
        output_path=str(PROJECT_ROOT / "data/processed/bronze/transactions"),
        table_name="transactions"
    )
    
    # aquí cargamos los datos de identidad
    load_to_bronze(
        spark=spark,
        input_path=str(PROJECT_ROOT / "data/raw/train_identity.csv"),
        output_path=str(PROJECT_ROOT / "data/processed/bronze/identity"),
        table_name="identity"
    )
    spark.stop()
    logger.info("Carga a la capa Bronze completada exitosamente")