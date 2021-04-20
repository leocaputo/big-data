#!/bin/bash

PASTA=/home/bigdata/repos/tarefa03
if [ ! -d "$PASTA" ]; then
    mkdir $PASTA
fi


cd $PASTA
wget ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/NOV/DNRES//DNBA2016.dbc
wget ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/NOV/DNRES//DNBA2017.dbc
wget ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/NOV/DNRES//DNBA2018.dbc
wget ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/NOV/DNRES//DNBA2019.dbc
