from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_activity_dashboard",
    default_args=default_args,
    schedule_interval="5 0 * * *",
)  # Запуск 5-го числа каждого месяца в 0:00


def process_data(**kwargs):
    # Чтение данных из файла profit_table.csv
    df = pd.read_csv("./data/profit_table.csv")

    # Получаем текущую дату для расчета флагов активности
    current_date = kwargs["execution_date"]
    result_df = transform(df, current_date.strftime("%Y-%m-%d"))

    # Сохранение результатов без перезаписи
    if not result_df.empty:
        if result_df.equals(pd.read_csv("./data/flags_activity.csv")):
            print("Файл не изменился. Обработка завершена.")
        else:
            result_df.to_csv(
                "./data/flags_activity.csv",
                mode="a",
                index=False,
                header=not kwargs["ti"].xcom_pull(task_ids="process_data"),
            )
            print("Файл обновлен.")

    # Сохранение результата в XCom для передачи дальше
    kwargs["ti"].xcom_push(key="result", value=result_df)


# Функция transform из transform_script.py
def transform(profit_table, date):
    start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date,
        freq="M").strftime(
        "%Y-%m-01"
    )

    df_tmp = (
        profit_table[profit_table["date"].isin(date_list)]
        .drop("date", axis=1)
        .groupby("id")
        .sum()
    )

    product_list = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    for product in tqdm(product_list):
        df_tmp[f"flag_{product}"] = df_tmp.apply(
            lambda x: x[f"sum_{product}"] != 0 and x[f"count_{product}"] != 0,
            axis=1
        ).astype(int)

    df_tmp = df_tmp.filter(regex="flag").reset_index()

    return df_tmp


# Создание оператора обработки данных
task_process_data = PythonOperator(
    task_id="process_data", python_callable=process_data, provide_context=True,
    dag=dag
)


# Создание оператора для проверки результата
def check_result(**kwargs):
    result_df = kwargs["ti"].xcom_pull(task_id="process_data")
    if not result_df.empty:
        print(f"Результат обработки:\n{result_df.head()}")
    else:
        print("Результат пустой")


# Создание оператора проверки результата
task_check_result = PythonOperator(
    task_id="check_result",
    python_callable=check_result,
    provide_context=True,
    dag=dag
)

# Зависимость между задачами
task_process_data >> task_check_result
