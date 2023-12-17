#!/bin/bash

# Crée le répertoire 'data_tmp' dans HDFS
hdfs dfs -mkdir -p /data_tmp

# Copie les fichiers du répertoire 'input' vers 'data_tmp' dans HDFS
hdfs dfs -cp /input/* /data_tmp/
