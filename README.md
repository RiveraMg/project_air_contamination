# Procesamiento de Datos en Tiempo Real con Kafka y Spark

## Proyecto
Implementación de procesamiento de datos en tiempo real utilizando **Apache Kafka** y **Apache Spark**, basado en un dataset simulado de departamentos, variables y promedios.

## Objetivo
Desarrollar un sistema de **streaming de datos en tiempo real** que permita:
- Enviar datos simulados desde un productor Kafka.
- Consumir y procesar los datos con Spark Streaming.
- Realizar transformaciones y análisis en tiempo real.

## Tecnologías
- **Apache Spark** (Structured Streaming)  
- **Apache Kafka** (Productor y Consumidor)  
- **Python 3.x**  
- **PySpark**  
- **VirtualBox** (opcional para entorno de pruebas)

## Estructura del Proyecto
```
/project-root
│
├─ producer.py # Productor Kafka que genera datos simulados
├─ consumer.py # Consumidor Kafka que recibe y procesa los datos
├─ spark_streaming.py # Spark Structured Streaming para análisis en tiempo real
└─ README.md # Documentación del proyecto

```

## Configuración
1. **Instalar Kafka y Spark**:
   - Kafka: [https://kafka.apache.org/quickstart](https://kafka.apache.org/quickstart)  
   - Spark: [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)  

2. **Instalar dependencias Python**:
```bash
pip install pyspark kafka-python
```

## Uso
1. Ejecutar el productor para enviar datos simulados:
```
python3 kafka_producer.py 
```

Ejecutar el consumidor para recibir y procesar datos:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 
spark_streaming_consumer_calidad_aire.py
```
## Autores

**Melanie Rivera** – Estudiante de Ingeniería de Sistemas
Proyecto académico – Curso de Big Data
