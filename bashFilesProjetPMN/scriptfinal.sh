#!/bin/bash

# Chemins vers les scripts bash nécessaires
VERIFY_INPUT_SCRIPT="./verify_input.sh"
COPY_DATA_INPUT_SCRIPT="./copie_data_input.sh"
COPY_DATA_TMP_SCRIPT="./copie_data_tmp.sh"
DELETE_TMP_SCRIPT="./delete_tmp.sh"
DELETE_CONTENT_INPUT_SCRIPT="./delete_content_input.sh"
RUN_SPARK_SCRIPT="./run_spark.sh"

# 1. Vérification du contenu du dépôt input dans HDFS
echo "Vérification du contenu du dépôt input..."
bash $VERIFY_INPUT_SCRIPT

# 2. Copie des fichiers de données dans le dépôt input
echo "Copie des fichiers de données dans le dépôt input..."
bash $COPY_DATA_INPUT_SCRIPT

# 3. Copie des fichiers du dépôt input dans un dossier temporaire data_tmp
echo "Copie des fichiers dans un dossier temporaire..."
bash $COPY_DATA_TMP_SCRIPT

# 4. Lancement de l'application Spark
echo "Lancement de l'application Spark..."
bash $RUN_SPARK_SCRIPT

# 5. Suppression du dossier temporaire data_tmp
echo "Suppression du dossier temporaire..."
bash $DELETE_TMP_SCRIPT

# 6. Suppression du contenu du dépôt input
echo "Suppression du contenu du dépôt input..."
bash $DELETE_CONTENT_INPUT_SCRIPT

echo "Script final terminé."
