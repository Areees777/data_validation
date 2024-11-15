# Data Validation Pipeline

Este proyecto implementa un pipeline de validación de datos utilizando PySpark. El objetivo es procesar archivos JSON, realizar validaciones en los datos, aplicar transformaciones, y escribir los resultados en HDFS o Kafka dependiendo de la validez de los registros.

## IMPORTANTE: 
En el proyecto vamos a encontrar lo siguiente:
- app: Encontramos la estructura del proyecto
    - configs: Aquí se encuentran todas las configuraciones que va a usar el programa
    - dependencias: Librerias que se usan durante la ejecución
    - spark: Este es el script que se lanza desde airflow. 
        Por qué hay una carpeta spark y con todo el codigo dentro? 
        La primera idea era crear un wheel subirlo a HDFS y lanzarlo mediante Spark, 
        pero no lo he conseguido, aunque era la primera idea que tenia. 
        De todos modos, para facilitarlo, he creado este script que se lanza directamente
        desde el contenedor de airflow. 
    - data: Donde guardé los datos de entrada que usa el proyecto
    - Resto de ficheros: 
        - El resto de ficheros son de poetry, y archivos que uso para gestionar el tema de
        mejores prácticas a la hora de desarrollar, como podría ser PEP8. 

## Estructura del Proyecto

El proyecto consta de las siguientes funcionalidades principales:

1. **Validación de Datos**:
   - Se validan campos específicos para asegurarse de que no estén vacíos o nulos.
   - Los registros se dividen en válidos e inválidos según las reglas de validación.

2. **Transformaciones**:
   - Se pueden agregar campos adicionales como la fecha y hora actual.

3. **Escritura de Resultados**:
   - Los registros válidos se envían a un tópico de Kafka.
   - Los registros inválidos se almacenan en HDFS en formato JSON.

## Configuración

### Variables de Entorno

El proyecto utiliza las siguientes variables de entorno:
- `PYSPARK_PYTHON`: Versión de Python para PySpark.
- `PYSPARK_DRIVER_PYTHON`: Versión de Python para el driver.

### Endpoints

- HDFS: `hdfs://hadoop:9000`
- Kafka: `kafka:9092`

## Dependencias

- Python 3.8 o superior
- PySpark
- Kafka
- HDFS

## Uso

### 1. Configuración de Entorno

Asegúrate de que las variables de entorno estén configuradas correctamente:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

### 2. Ejecución
```bash
python main.py
```

### 3. Entrada y salida de datos
Entrada: Los datos de entrada deben estar en formato JSONL y ubicarse en /data/input/events/person/.

Salida:
Registros válidos: Se envían a los tópicos de Kafka.
Registros inválidos: Se escriben en /data/output/discards/person/ en HDFS.

## Funcionalidad
### 1. Validación de Campos
Aplica las reglas de validación a los datos y los divide en registros válidos e inválidos.
```bash
apply_validations(df: DataFrame, validations: dict) -> (DataFrame, DataFrame)
```

### 2. Adición de Campos
Agrega campos adicionales al DataFrame.
```bash
add_fields(df: DataFrame, field_name: str, function: str) -> DataFrame
```

### 3. Escritura en HDFS
Escribe datos en HDFS.
```bash
write_to_hdfs(df: DataFrame, file_path: str, file_format: str, file_save_mode: str)
```

### 4. Escritura en Kafka
Envía datos a un tópico de Kafka.
```bash
write_to_kafka(df: DataFrame, topic: str)
```

## Metadatos del Programa
El flujo de datos está definido en PROGRAM_METADATA, que especifica las fuentes, transformaciones y sinks.
