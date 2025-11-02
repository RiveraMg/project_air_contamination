# Importar librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName('CalidadAire_Colombia').getOrCreate()

# Ruta del archivo en HDFS
file_path = 'hdfs://localhost:9000/user/hadoop/calidad_aire/calidad_aire.csv'

# Leer el archivo CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Mostrar esquema del DataFrame
df.printSchema()

# Mostrar primeras filas
df.show(5)

print("Columnas del DataFrame:")
print(df.columns)

# -------------------------------
# Renombrar columnas problemátic
# -------------------------------
df = df.withColumnRenamed("No. de datos", "numero_datos") \
       .withColumnRenamed("Tiempo de exposición (horas)", "tiempo_exposicion_horas") \
       .withColumnRenamed("Porcentaje excedencias limite actual", "porcentaje_excedencias_limite_actua>
       .withColumnRenamed("Nombre del departamento", "nombre_del_departamento") \
       .withColumnRenamed("Autoridad ambiental", "autoridad_ambiental") \
       .withColumnRenamed("Variable", "variable") \
       .withColumnRenamed("Promedio", "promedio") \
       .withColumnRenamed("Excedencias limite actual", "excedencias_limite_actual") \
       .withColumnRenamed("Id estación", "id_estacion")

# Confirmar nombres corregidos
print("Columnas renombradas:")
print(df.columns)

# Estadísticas básicas
df.summary().show()

# -------------------------------
# CONSULTAS ESPECÍFICAS
# -------------------------------

# Filtrar registros con promedio mayor a 100
print("Registros con promedio mayor a 100:\n")
promedio_alto = df.filter(F.col('promedio') > 100).select('autoridad_ambiental', 'nombre_del_departame>
promedio_alto.show()

# Contar cuántas estaciones hay por departamento
print("Número de estaciones por departamento:\n")
estaciones_por_departamento = df.groupBy('nombre_del_departamento').agg(F.countDistinct('id_estacion')>
estaciones_por_departamento.show(10)

# Promedio general por variable medida
print("Promedio general por variable:\n")
promedio_variable = df.groupBy('variable').agg(F.avg('promedio').alias('promedio_general'))
promedio_variable.show(10)

# Departamentos con mayor número de excedencias del límite actual
print("Departamentos con mayor número de excedencias del límite actual:\n")
excedencias = df.groupBy('nombre_del_departamento').agg(F.sum('excedencias_limite_actual').alias('tota>
excedencias.show(10)