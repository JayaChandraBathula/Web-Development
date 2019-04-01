import os
import csv
import itertools

from datetime import datetime, timedelta
from newsapi import NewsApiClient


from airflow import DAG
from airflow import settings
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 22),
    'email': ['david.o@ieee.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


API_KEY = os.environ["NEWS_API_KEY"]

# DAG Object
dag = DAG('tempus_challenge_dag',
          default_args=default_args,
          schedule_interval='0 0 * * *',
          catchup=False)

def newsretrieve_fromapi(self):
    newsapi = NewsApiClient(api_key='616610a29594476181bd38b685362b91')# /v2/sources
    allengnewssources = newsapi.get_sources(language='en')
    print(len(allengnewssources['sources']))
    
    englishnewssourceslist=[]
    for i in range(0,len(allengnewssources['sources'])):
        englishnewssourceslist.append(allengnewssources['sources'][i]['id']) 
    sourceheadlinesdict={}
    for i in englishnewssourceslist:
        templist=[]
        # /v2/top-headlines
        top_headlines = newsapi.get_top_headlines(sources=i,language='en')
        for j in range(0,len(top_headlines['articles'])):
            templist.append(top_headlines['articles'][j]['title'])
            sourceheadlinesdict[i]=templist
  

# define workflow tasks
# begin workflow
start_task = DummyOperator(task_id='start', dag=dag)


#retrieving data from api and storing it in dictionary
newsretrieval_task = PythonOperator(task_id='create_storage_task',
                                         provide_context=True,
                                         python_callable=newsretrieve_fromapi,
                                         retries=3,
                                         dag=dag)

# end workflow
end_task = DummyOperator(task_id='end', dag=dag)

start_task >> newsretrieval_task >> end_task