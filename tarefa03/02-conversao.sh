#!/bin/bash

cd /home/bigdata/repos/tarefa03/dbc2csv
#echo 01 | sudo -S docker run -it -v /home/bigdata/repos/tarefa03:/usr/src/app/data dbc2csv make

sudo docker run -it -v /home/bigdata/repos/tarefa03:/usr/src/app/data dbc2csv make

cd /home/bigdata/repos/tarefa03/
hdfs dfs -put DNBA2016.dbc DNBA2017.dbc DNBA2018.dbc DNBA2019.dbc /raw/dbc
sudo rm DNBA2016.dbc DNBA2017.dbc DNBA2018.dbc DNBA2019.dbc
sudo rm DNBA2016.dbf DNBA2017.dbf DNBA2018.dbf DNBA2019.dbf

cd /home/bigdata/repos/tarefa03/csv
sudo mv DNBA2016.csv base_sinasc_2016_v1.csv
sudo mv DNBA2017.csv base_sinasc_2017_v1.csv
sudo mv DNBA2018.csv base_sinasc_2018_v1.csv
sudo mv DNBA2019.csv base_sinasc_2019_v1.csv

hdfs dfs -put base_sinasc_2016_v1.csv base_sinasc_2017_v1.csv base_sinasc_2018_v1.csv base_sinasc_2019_v1.csv /refined/csv

sudo rm base_sinasc_2016_v1.csv base_sinasc_2017_v1.csv base_sinasc_2018_v1.csv base_sinasc_2019_v1.csv
