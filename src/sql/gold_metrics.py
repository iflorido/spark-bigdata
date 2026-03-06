from pyspark.sql import SparkSession
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)    

PROJECT_ROOT = Path(__file__).resolve().parents[2] # Ajusta esta ruta según la estructura de tu proyecto

def create_spark_session(app_name: str) -> SparkSession:
    
    """ Creamos una sesión de Spark con el nombre de la 
    aplicación proporcionado"""
    
    return SparkSession.builder.appName(app_name).master("local[*]").config("spark.driver.memory", "4g").getOrCreate()  

def register_silver_views(spark: SparkSession, silver_path: str):  
    """
    Registramos vistas temporales para facilitar consultas SQL en la capa Silver.
    """
    logger.info("Registrando vistas temporales para la capa Silver")
    
    # Registramos la vista
    
    df = spark.read.parquet(silver_path)
    df.createOrReplaceTempView("transactions")
    logger.info(f"Vista temporal 'transactions' registrada {df.count()} filas para la capa Silver")
    return df

def compute_gold_metrics(spark: SparkSession, output_base: str):
    """
    Calculamos métricas clave para la capa Gold, como el total de transacciones, monto promedio y tasa de fraude.
    """
    logger.info("Calculando métrica 1: fraude por hora del día")
    # Métrica 1: Tasa de fraude por hora del día, creamos la consulta SQL para calcular la tasa de fraude por hora del día, incluyendo el número total de transacciones, el número de fraudes y el importe medio por hora.
    df_hora = spark.sql("""
        SELECT
            transaction_hour,
            COUNT(*) AS total_transacciones,
            SUM(isFraud) AS total_fraudes,
            ROUND(SUM(isFraud) * 100.0 / COUNT(*), 2) AS pct_fraude,
            ROUND(AVG(TransactionAmt), 2) AS importe_medio
        FROM transactions
        GROUP BY transaction_hour
        ORDER BY transaction_hour
    """)
    df_hora.show(24)
    df_hora.write.mode("overwrite").parquet(f"{output_base}/gold/metrics/fraude_por_hora")
    logger.info("Métrica 1 guardada en la capa Gold: fraude por hora del día")
    
    # Metrica 2: Tasa de fraude por tipo de trajeta de credito, creamos la consulta SQL para calcular la tasa de fraude por tipo de tarjeta de crédito, incluyendo el número total de transacciones, el número de fraudes y el importe medio por tipo de tarjeta.
    logger.info("Calculando métrica 2: fraude por tipo de tarjeta de crédito")
    df_tarjeta = spark.sql("""
        SELECT
            card4 AS red_tarjeta,
            card6 AS tipo_tarjeta,
            COUNT(*) AS total_transacciones,
            SUM(isFraud) AS total_fraudes,
            ROUND(SUM(isFraud) * 100.0 / COUNT(*), 2) AS pct_fraude,
            ROUND(AVG(TransactionAmt), 2) AS importe_medio,
            ROUND(AVG(CASE WHEN isFraud = 1 THEN TransactionAmt END), 2) AS importe_medio_fraude
        FROM transactions
        WHERE card4 != 'unknown'
        GROUP BY card4, card6
        ORDER BY pct_fraude DESC
    """)
    df_tarjeta.show()
    df_tarjeta.write.mode("overwrite").parquet(f"{output_base}/gold/metrics/fraude_por_tipo_tarjeta")
    logger.info("Métrica 2 guardada en la capa Gold: fraude por tipo de tarjeta de crédito")
    
    # Metrica 3: Tasa de frauder por categoria de producto, creamos la consulta SQL para calcular la tasa de fraude por categoría de producto, incluyendo el número total de transacciones, el número de fraudes y el importe medio por categoría de producto.
    logger.info("Calculando métrica 3: fraude por categoría de producto")
    df_producto = spark.sql(
        "SELECT ProductCD, "
        "COUNT(*) AS total_transacciones, "
        "SUM(isFraud) AS total_fraudes, "
        "ROUND(SUM(isFraud) * 100.0 / COUNT(*), 2) AS pct_fraude, "
        "ROUND(AVG(TransactionAmt), 2) AS importe_medio, "
        "ROUND(MAX(TransactionAmt), 2) AS importe_maximo "
        "FROM transactions "
        "GROUP BY ProductCD "
        "ORDER BY pct_fraude DESC"
    )
    df_producto.show()
    df_producto.write.mode("overwrite").parquet(f"{output_base}/gold/metrics/fraude_por_categoria_producto")
    logger.info("Métrica 3 guardada en la capa Gold: fraude por categoría de producto")
    
    # Metrica 4 : Evolución mensual de fraudes, Intentamos calcular la evolución mensual de fraudes, incluyendo el número total de transacciones, el número de fraudes y el importe medio por mes.
    logger.info("Calculando métrica 4: evolución mensual de fraudes")
    df_mensual = spark.sql("""
        SELECT
            transaction_month,
            COUNT(*) AS total_transacciones,
            SUM(isFraud) AS total_fraudes,
            ROUND(SUM(isFraud) * 100.0 / COUNT(*), 2) AS pct_fraude,
            ROUND(SUM(TransactionAmt), 2) AS volumen_total,
            ROUND(SUM(CASE WHEN isFraud = 1 THEN TransactionAmt ELSE 0 END), 2) AS volumen_fraude
            FROM transactions GROUP BY transaction_month
            ORDER BY transaction_month
    """)
    df_mensual.show()
    df_mensual.write.mode("overwrite").parquet(f"{output_base}/gold/metrics/evolucion_mensual_fraudes")
    logger.info("Métrica 4 guardada en la capa Gold: evolución mensual de fraudes")
    
if __name__ == "__main__":
    spark = create_spark_session("FinancialPipeline-Gold")

    silver_path = str(PROJECT_ROOT / "data/processed/silver/transactions_enriched")
    output_base = str(PROJECT_ROOT / "data/aggregated/gold")

    register_silver_views(spark, silver_path)
    compute_gold_metrics(spark, output_base)

    spark.stop()
    logger.info("Gold layer completada ✅")
    