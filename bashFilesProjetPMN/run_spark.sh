#!/bin/bash

# Exécute l'application Spark en utilisant spark-submit
# --class Main : Spécifie la classe principale à exécuter
# --master yarn : Spécifie le mode de gestionnaire de ressources (ici YARN)
# --deploy-mode cluster : Exécute l'application sur le cluster plutôt que sur la machine locale
# Remplacer '/chemin/vers/votre/application.jar' par le chemin réel de votre fichier JAR
spark-submit \
  --class main.Main \
  --master yarn \
  --deploy-mode cluster \
  /home/simonlinux/ProjetHadoopSpark-1.0-SNAPSHOT.jar
