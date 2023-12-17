#!/bin/bash

# Vérifie si le répertoire 'input' existe dans HDFS
hdfs dfs -test -e /input
if [ $? -eq 0 ]; then
    # Compte le nombre de fichiers dans le répertoire 'input'
    if [ $(hdfs dfs -count /input | awk '{print $2}') -eq 0 ]; then
        echo "Le répertoire input est vide."
    else
        echo "Le répertoire input contient des fichiers."
    fi
else
    echo "Le répertoire input n'existe pas dans HDFS."
fi
