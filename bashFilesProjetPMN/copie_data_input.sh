#!/bin/bash

# Crée le répertoire 'input' dans HDFS s'il n'existe pas
hdfs dfs -mkdir -p /input

# Copie les fichiers depuis un répertoire local vers le répertoire 'input' dans HDFS
# Remplacer '/chemin/local/data/*' par le chemin réel de vos fichiers de données
hdfs dfs -put /home/simonlinux/datasetsProjetPMN/* /input/
