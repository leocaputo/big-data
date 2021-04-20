#!/bin/bash

#Add the file and stage it for commit
git add 00-ApagaTudo.sh
git add 01-download_bases.sh
git add 02-conversao.sh
git add 03-extracao.sh
git add 03-extracao.py
git add 04-exploracao.sh
git add 04-exploracao.py
git add dag-tarefa03.py


#Commit the file to your local repository
git commit -m 00-ApagaTudo.sh
git commit -m 01-download_bases.sh
git commit -m 02-conversao.sh
git commit -m 03-extracao.sh
git commit -m 03-extracao.py
git commit -m 04-exploracao.sh
git commit -m 04-exploracao.py
git commit -m dag-tarefa03.py

#Push the changes to Github
git push
