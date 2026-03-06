from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
from pathlib import Path
from pyspark.sql import types as T


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2] # Ajusta esta ruta según la estructura de tu proyecto


def create_spark_session(app_name: str) -> SparkSession:
    
    """ Creamos una sesión de Spark con el nombre de la 
    aplicación proporcionado"""
    
    return SparkSession.builder.appName(app_name).master("local[*]").config("spark.driver.memory", "4g").getOrCreate()

def transform_transactions(df):
    """
    Limpiamos y transformamos los datos del DataFrame en transacciones
    """
    logger.info("Transformando datos de transacciones")
    
    fecha_base = F.to_timestamp(F.lit("2017-11-30"), "yyyy-MM-dd")
    
    df = df.withColumn(
        "transaction_date",
        (fecha_base.cast("long") + F.col("TransactionDT")).cast("timestamp")
    )
    # Extraemos características temporales para análisis posterior, pasamos de timestamp a hora, día y mes
    df = df.withColumn("transaction_hour", F.hour("transaction_date")).withColumn("transaction_day", F.dayofmonth("transaction_date")).withColumn("transaction_month", F.month("transaction_date"))
    
    # Rellenamos valores faltantes con placeholders o valores indicativos para evitar problemas en análisis posteriores.
    df = df.fillna({
        "P_emaildomain": "unknown",   # email desconocido
        "R_emaildomain": "no_recipient",  # sin destinatario
        "card4": "unknown",
        "card6": "unknown",
        "dist1": -1.0,   # -1 indica "no disponible" (mejor que 0, que es válido)
        "dist2": -1.0
    })
    
    # Seleccion de columnas relevantes para análisis posteriores, incluyendo la marca de tiempo de ingestión para rastreo.
    columnas_silver = [
        "TransactionID", "transaction_date", "transaction_hour", "transaction_day", "transaction_month",
        "TransactionAmt", "ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain",
        "dist1", "dist2", "isFraud", "ingestion_timestamp"
    ]
    
    df = df.select(columnas_silver) # Seleccionamos solo las columnas relevantes para la capa Silver
    
    logger.info(f"Transformación de transacciones completada: {df.count()} filas procesadas, {len(df.columns)} columnas") # Logueamos el número de filas y columnas después de la transformación
    return df

def transform_identity(df):
    """
    Limpiamos y transformamos los datos del DataFrame en identidades
    """
    logger.info("Transformando datos de identidades")
    
    # Rellenamos valores faltantes con placeholders o valores indicativos para evitar problemas en análisis posteriores.
    df = df.fillna({
        "id_12": "unknown",
        "id_15": "unknown",
        "id_16": "unknown",
        "id_28": "unknown",
        "id_29": "unknown",
        "DeviceType": "unknown",
        "DeviceInfo": "unknown"
    })
    
    # Seleccion de columnas relevantes para análisis posteriores, incluyendo la marca de tiempo de ingestión para rastreo.
    columnas_silver = [
        "TransactionID", "id_12", "id_13", "id_14", "id_15", 
        "id_16", "id_17", "id_18", "id_19", "id_20", 
        "id_21", "ingestion_timestamp"
    ]
       
    logger.info(f"Transformación de identidades completada: {df.count()} filas procesadas, {len(df.columns)} columnas") # Logueamos el número de filas y columnas después de la transformación
    return df

# ahora hacemos el join entre transacciones e identidades para crear una tabla Silver unificada

def join_transactions_identity(df_transactions, df_identity):
    """
    Realizamos un join entre las transacciones y las identidades para crear una tabla Silver unificada
    """
    logger.info("Realizando join entre transacciones e identidades para crear tabla Silver unificada")
    df_identity = df_identity.drop("ingestion_timestamp") # Eliminamos la columna de timestamp de identidad para evitar confusiones en el join
    df_joined = df_transactions.join(df_identity, on="TransactionID", how="left")
    
    # Revisamos cuantos datos de transacciones tienen datos de identidad
    
    con_identidad = df_joined.filter(F.col("DeviceType").isNotNull()).count()
    total_transacciones = df_joined.count()
    logger.info(f"De {total_transacciones} transacciones, {con_identidad} tienen datos de identidad asociados ({(con_identidad/total_transacciones)*100:.2f})%")
    
    return df_joined

if __name__ == "__main__":
    spark = create_spark_session("SilverTransformer-App-Finanzas")
    
    # Cargamos los datos de la capa Bronze
    logger.info("Cargando datos de la capa Bronze")
    
    df_transactions = spark.read.parquet(str(PROJECT_ROOT / "data/processed/bronze/transactions"))
    df_identity = spark.read.parquet(str(PROJECT_ROOT / "data/processed/bronze/identity"))
    
    df_transactions = transform_transactions(df_transactions)
    df_identity = transform_identity(df_identity)
    
    # hacemos la union 
    df_silver = join_transactions_identity(df_transactions, df_identity)
    output_path = str(PROJECT_ROOT / "data/processed/silver/transactions_enriched")
    df_silver.write.mode("overwrite").parquet(output_path)
    logger.info(f"Silver layer guardada en {output_path} ok")
    spark.stop()
    