#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from datetime import date, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# [START import_module]
from dotenv import load_dotenv

from dags.modules.collect.khutoday import KhutodayCollector
from dags.modules.collect.swcon import SwconCollector
from dags.modules.collect.swedu import SweduCollector
from dags.modules.database.pymongo import PymongoClient
from dags.modules.vectorstore.pinecone import PineconeClient

load_dotenv(verbose=True)

PINECONE_API_KEY = os.environ["PINECONE_API_KEY"]
PINECONE_ENVIRONMENT_REGION = os.environ["PINECONE_ENVIRONMENT_REGION"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "khugpt",
    "depends_on_past": False,
    "email": ["syw5141@khu.ac.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "khugpt-dag",
    default_args=default_args,
    description="A DAG for KHUGPT.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["service"],
)
# [END instantiate_dag]

# [START basic_task]


def print_test(context):
    print(f"test: {context}")


def collector_khu_today(context: str):
    assert context == "khu_today"
    collector = KhutodayCollector
    docs = collector.collect()
    if docs:
        collector.upload_db()
    print(f"collect {context}: {len(docs)} documents at {str(date.today())}")


def collector_swedu(context: str) -> None:
    assert context == "swedu"
    collector = SweduCollector()
    docs = collector.collect()
    if docs:
        collector.upload_db()
    print(f"collect {context}: {len(docs)} documents at {str(date.today())}")


def collector_swcon(context: str) -> None:
    assert context == "swcon"
    collector = SwconCollector()
    docs = collector.collect()
    if docs:
        collector.upload_db()
    print(f"collect {context}: {len(docs)} documents at {str(date.today())}")


def transformer_vector(context: str) -> None:
    assert context == "vector"
    client = PymongoClient()
    pinecone_client = PineconeClient(
        index_name="khugpt",
        pinecone_api_key=PINECONE_API_KEY,
        environment=PINECONE_ENVIRONMENT_REGION,
        openai_api_key=OPENAI_API_KEY,
    )
    collection = client["khugpt"]["documents"]
    res = collection.find({})
    docs = []
    for doc in res:
        docs.append(doc)

    if docs:
        pinecone_client.upsert_documents(docs)

    print(f"transform & upload {context}: {len(docs)} documents at {str(date.today())}")


collect_khu_today = PythonOperator(
    task_id="collect_khu_today",
    python_callable=collector_khu_today,
    op_kwargs={"context": "khu_today"},
    dag=dag,
)
collect_swedu = PythonOperator(
    task_id="collect_swedu",
    python_callable=collector_swedu,
    op_kwargs={"context": "swedu"},
    dag=dag,
)
collect_swcon = PythonOperator(
    task_id="collect_swcon",
    python_callable=collector_swcon,
    op_kwargs={"context": "swcon"},
    dag=dag,
)

aggregate_data = PythonOperator(
    task_id="aggregate_data",
    python_callable=print_test,
    op_kwargs={"context": "mongodb"},
    dag=dag,
)

transform_vector = PythonOperator(
    task_id="transform_vector",
    python_callable=transformer_vector,
    op_kwargs={"context": "vector"},
    dag=dag,
)

report = PythonOperator(
    task_id="report",
    python_callable=print_test,
    op_kwargs={"context": "report"},
    dag=dag,
)

(
    [collect_khu_today, collect_swcon, collect_swedu]
    >> aggregate_data
    >> transform_vector
    >> report
)
