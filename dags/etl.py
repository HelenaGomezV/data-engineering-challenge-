from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sys
import os

# Para importar la función de KPIs
sys.path.append('/opt/airflow/src')
from kpis import calculate_kpis

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/output"

def read_data():
    patients = pd.read_csv(f"{DATA_DIR}/Patients.csv")
    trials = pd.read_csv(f"{DATA_DIR}/Clinical_Trial.csv")
    relations = pd.read_csv(f"{DATA_DIR}/Patients_Trials.csv")

    patients.to_csv(f"{OUTPUT_DIR}/raw_patients.csv", index=False)
    trials.to_csv(f"{OUTPUT_DIR}/raw_trials.csv", index=False)
    relations.to_csv(f"{OUTPUT_DIR}/raw_relations.csv", index=False)

def merge_data():
    patients = pd.read_csv(f"{OUTPUT_DIR}/raw_patients.csv")
    trials = pd.read_csv(f"{OUTPUT_DIR}/raw_trials.csv")
    relations = pd.read_csv(f"{OUTPUT_DIR}/raw_relations.csv")

    # Realizar joins
    df = relations.merge(patients, on="patient_id", how="left")
    df = df.merge(trials, on="trial_id", how="left")

    # Renombrar columnas de fechas para estandarizar
    df.rename(columns={
        "study_start_date": "start_date",
        "study_end_date": "end_date"
    }, inplace=True)

    # Guardar el resultado
    df.to_csv(f"{OUTPUT_DIR}/merged.csv", index=False)



def clean_data():
    df = pd.read_csv(f"{OUTPUT_DIR}/merged.csv")
    
    print("Columnas disponibles:", df.columns.tolist()) 
    
    df.drop_duplicates(inplace=True)

    required_cols = ["patient_id", "trial_id", "start_date", "end_date"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Faltan columnas necesarias: {missing_cols}")
    
    df.dropna(subset=required_cols, inplace=True)
    df.to_csv(f"{OUTPUT_DIR}/clean_data.csv", index=False)

# DAG definition
default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

with DAG("etl_local_pipeline",
         default_args=default_args,
         schedule_interval=None,
         description="ETL por fases: lectura, unión, limpieza, KPIs") as dag:

    t1 = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    t2 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data
    )

    t3 = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    t4 = PythonOperator(
        task_id="calculate_kpis",
        python_callable=calculate_kpis
    )

    t1 >> t2 >> t3 >> t4

