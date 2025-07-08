import pandas as pd

INPUT = "/opt/airflow/output/clean_data.csv"
OUTPUT = "/opt/airflow/output/kpis.csv"

def patients_per_country(df):
    return df.groupby("country")["patient_id"].nunique().to_dict()

def avg_age_per_diagnosis(df):
    return df.groupby("diagnosis")["age"].mean().round(1).dropna().to_dict()

def most_used_medications(df, top_n=5):
    return df["medication"].value_counts().head(top_n).to_dict()

def avg_trial_duration(df):
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df["duration"] = (df["end_date"] - df["start_date"]).dt.days
    return round(df["duration"].mean(), 2)

def avg_days_between_trials(df):
    df["start_date"] = pd.to_datetime(df["start_date"])
    deltas = []
    for _, group in df.groupby("patient_id"):
        group = group.sort_values("start_date")
        if len(group) > 1:
            diff = group["start_date"].diff().dt.days.dropna()
            deltas.extend(diff)
    return round(pd.Series(deltas).mean(), 2) if deltas else 0

def percent_incomplete_trials(df):
    return round(100 * df[df.isnull().any(axis=1)].shape[0] / df.shape[0], 2)

def percent_multi_trial_patients(df):
    trial_counts = df.groupby("patient_id")["trial_id"].nunique()
    multi = trial_counts[trial_counts > 1].count()
    return round(100 * multi / len(trial_counts), 2)

def calculate_kpis():
    df = pd.read_csv(INPUT)
    results = {
        "patients_per_country": patients_per_country(df),
        "avg_age_per_diagnosis": avg_age_per_diagnosis(df),
        "most_used_medications": most_used_medications(df),
        "avg_trial_duration_days": avg_trial_duration(df),
        "avg_days_between_trials": avg_days_between_trials(df),
        "percent_incomplete_trials": percent_incomplete_trials(df),
        "percent_multi_trial_patients": percent_multi_trial_patients(df),
    }
    # Exportar como una fila JSON-compatible
    pd.DataFrame([results]).to_csv(OUTPUT, index=False)

if __name__ == "__main__":
    calculate_kpis()
    print(f"KPIs calculados y guardados en {OUTPUT}")
