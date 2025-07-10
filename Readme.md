# Data Validation Pipeline

This project implements a data validation pipeline using **PySpark**. The goal is to process JSON files, perform data validations, apply transformations, and write the results to **HDFS** or **Kafka** depending on the validity of the records.

## IMPORTANT:  
The project contains the following structure:

- **app**: The main project structure  
  - **configs**: Contains all the configurations used by the program  
  - **dependencies**: Libraries used during execution  
  - **spark**: This is the script triggered from Airflow.  
    Why is there a `spark` folder with all the code inside?  
    Initially, the plan was to create a wheel, upload it to HDFS, and launch it via Spark, but I couldn’t get it working.  
    Instead, to simplify, I created this script which is run directly from the Airflow container.  
  - **data**: Where input data used by the project is stored  
  - **Other files**:  
    - Poetry files and configuration files to manage best practices in development, such as PEP8 compliance  

## Project Structure

The project includes the following main functionalities:

1. **Data Validation**  
   - Specific fields are validated to ensure they are neither empty nor null.  
   - Records are split into valid and invalid based on validation rules.

2. **Transformations**  
   - Additional fields can be added, such as the current date and time.

3. **Result Writing**  
   - Valid records are sent to a Kafka topic.  
   - Invalid records are stored in HDFS in JSON format.

## Configuration

### Environment Variables

The project uses the following environment variables:  
- `PYSPARK_PYTHON`: Python version for PySpark  
- `PYSPARK_DRIVER_PYTHON`: Python version for the driver

### Endpoints

- HDFS: `hdfs://hadoop:9000`  
- Kafka: `kafka:9092`

## Dependencies

- Python 3.8 or higher  
- PySpark  
- Kafka  
- HDFS

## Usage

### 1. Environment Setup

Make sure the environment variables are correctly set:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
