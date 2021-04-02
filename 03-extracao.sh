#!/bin/bash

source /home/bigdata/.bashrc
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

spark-submit --master spark://node5:7077 /home/bigdata/repos/tarefa03/03-extracao.py

