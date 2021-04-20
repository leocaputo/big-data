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

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='Grupo03_Tarefa03',
    default_args=args,
    schedule_interval='45 19 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)


# [START howto_operator_bash]

apagar = BashOperator(
    task_id='apagar',
    bash_command='sh /home/bigdata/repos/tarefa03/00-ApagaTudo.sh ',
    dag=dag,
)


download = BashOperator(
    task_id='download_bases',
    bash_command='sh /home/bigdata/repos/tarefa03/01-download_bases.sh ',
    dag=dag,
)


conversao = BashOperator(
    task_id='conversao',
    bash_command='sh /home/bigdata/repos/tarefa03/02-conversao.sh ',
    dag=dag,
)


extracao = BashOperator(
    task_id='extracao',
    bash_command='sh /home/bigdata/repos/tarefa03/03-extracao.sh ',
    dag=dag,
)


exploracao = BashOperator(   
    task_id='exploracao',
    bash_command='sh /home/bigdata/repos/tarefa03/04-exploracao.sh ',
    dag=dag,
)



# [END howto_operator_bash]


apagar >> download >> conversao >> extracao >> exploracao

#for i in range(5):
#    task = BashOperator(
#        task_id='runme_' + str(i),
#        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
#        dag=dag,
#    )
#    task >> run_this

# [START howto_operator_bash_template]
#also_run_this = BashOperator(
#    task_id='also_run_this',
#    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
#    dag=dag,
#)
# [END howto_operator_bash_template]
#also_run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()
