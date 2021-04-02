#!/bin/bash

#REMOVER ARQUIVOS NO LINUX

PASTA1=/home/bigdata/repos/tarefa03
PASTA2=/home/bigdata/repos/tarefa03/csv
ARQ1=DNBA2016.dbc
ARQ2=DNBA2017.dbc
ARQ3=DNBA2018.dbc
ARQ4=DNBA2019.dbc
ARQ5=DNBA2016.dbf
ARQ6=DNBA2017.dbf
ARQ7=DNBA2018.dbf
ARQ8=DNBA2019.dbf
ARQ9=base_sinasc_2016_v1.csv
ARQ10=base_sinasc_2017_v1.csv
ARQ11=base_sinasc_2018_v1.csv
ARQ12=base_sinasc_2019_v1.csv


if [ -d "$PASTA1" ]; then
	cd $PASTA1
	[ -f "$ARQ1" ] && sudo rm -Rfv $ARQ1
	[ -f "$ARQ2" ] && sudo rm -Rfv $ARQ2
	[ -f "$ARQ3" ] && sudo rm -Rfv $ARQ3
	[ -f "$ARQ4" ] && sudo rm -Rfv $ARQ4
	[ -f "$ARQ5" ] && sudo rm -Rfv $ARQ5
	[ -f "$ARQ6" ] && sudo rm -Rfv $ARQ6
	[ -f "$ARQ7" ] && sudo rm -Rfv $ARQ7
	[ -f "$ARQ8" ] && sudo rm -Rfv $ARQ8
fi


if [ -d "$PASTA2" ]; then
        cd $PASTA2
	[ -f "$ARQ9" ] && sudo rm -Rfv $ARQ9
	[ -f "$ARQ10" ] && sudo rm -Rfv $ARQ10
	[ -f "$ARQ11" ] && sudo rm -Rfv $ARQ11
	[ -f "$ARQ12" ] && sudo rm -Rfv $ARQ12	
fi



#REMOVER ARQUIVOS NO HDFS

HDFS1=/raw/dbc/DNBA2016.dbc
HDFS2=/raw/dbc/DNBA2017.dbc
HDFS3=/raw/dbc/DNBA2018.dbc
HDFS4=/raw/dbc/DNBA2019.dbc
HDFS5=/refined/csv/base_sinasc_2016_v1.csv
HDFS6=/refined/csv/base_sinasc_2016_recortada_v1.csv
HDFS7=/refined/csv/base_sinasc_2017_v1.csv
HDFS8=/refined/csv/base_sinasc_2017_recortada_v1.csv
HDFS9=/refined/csv/base_sinasc_2018_v1.csv
HDFS10=/refined/csv/base_sinasc_2018_recortada_v1.csv
HDFS11=/refined/csv/base_sinasc_2019_v1.csv
HDFS12=/refined/csv/base_sinasc_2019_recortada_v1.csv
HDFS13=/refined/csv/base_sinasc_2016_a_2019_recortada_v1.csv




if hdfs dfs -test -e $HDFS1; then
    hdfs dfs -rm -R -skipTrash $HDFS1
fi

if hdfs dfs -test -e $HDFS2; then
    hdfs dfs -rm -R -skipTrash $HDFS2
fi

if hdfs dfs -test -e $HDFS3; then
    hdfs dfs -rm -R -skipTrash $HDFS3
fi

if hdfs dfs -test -e $HDFS4; then
    hdfs dfs -rm -R -skipTrash $HDFS4
fi

if hdfs dfs -test -e $HDFS5; then
    hdfs dfs -rm -R -skipTrash $HDFS5
fi

if hdfs dfs -test -e $HDFS6; then
    hdfs dfs -rm -R -skipTrash $HDFS6
fi

if hdfs dfs -test -e $HDFS7; then
    hdfs dfs -rm -R -skipTrash $HDFS7
fi

if hdfs dfs -test -e $HDFS8; then
    hdfs dfs -rm -R -skipTrash $HDFS8
fi

if hdfs dfs -test -e $HDFS9; then
    hdfs dfs -rm -R -skipTrash $HDFS9
fi

if hdfs dfs -test -e $HDFS10; then
    hdfs dfs -rm -R -skipTrash $HDFS10
fi

if hdfs dfs -test -e $HDFS11; then
    hdfs dfs -rm -R -skipTrash $HDFS11
fi

if hdfs dfs -test -e $HDFS12; then
    hdfs dfs -rm -R -skipTrash $HDFS12
fi

if hdfs dfs -test -e $HDFS13; then
    hdfs dfs -rm -R -skipTrash $HDFS13
fi

