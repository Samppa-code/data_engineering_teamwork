from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import pandas as pd
import sqlite3
import zipfile
import os
from kaggle.api.kaggle_api_extended import KaggleApi