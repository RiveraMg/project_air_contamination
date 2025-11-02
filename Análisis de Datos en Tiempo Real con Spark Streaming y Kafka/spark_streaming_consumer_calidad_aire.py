from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, FloatType, LongType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("CalidadAireStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos que llegan desde Kafka
schema = StructType() \
    .add("nombre_del_departamento", StringType()) \
    .add("variable", StringType()) \
    .add("promedio", FloatType()) \
    .add("timestamp", LongType())

# Leer el flujo de datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Convertir el valor (JSON) en columnas individuales
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Calcular el promedio de contaminación por tipo de variable
promedio_por_variable = df_parsed.groupBy("variable").agg(avg("promedio").alias("promedio_medio"))

# Mostrar resultados en consola cada 5 segundos
query = promedio_por_variable.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
