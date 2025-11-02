import time
import json
import random
from kafka import KafkaProducer

# Listas basadas en tu dataset
departamentos = [
    "Antioquia", "Cundinamarca", "Atlántico", "Valle del Cauca", 
    "Santander", "Bolívar", "Boyacá", "Tolima", "Meta", "Cesar"
]

variables = [
    "PM10", "PM2.5", "Ozono", "NO2", "SO2", "CO"
]

# Función para generar un registro simulado
def generar_dato_calidad_aire():
    return {
        "nombre_del_departamento": random.choice(departamentos),
        "variable": random.choice(variables),
        "promedio": round(random.uniform(10, 150), 2),  # rango de contaminación
        "timestamp": int(time.time())
    }

# Configurar el productor Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Enviando datos simulados de calidad del aire al tópico 'sensor_data'...\n")

# Enviar datos cada segundo
while True:
    dato = generar_dato_calidad_aire()
    producer.send('sensor_data', value=dato)
    print(f"Enviado: {dato}")
    time.sleep(1)
