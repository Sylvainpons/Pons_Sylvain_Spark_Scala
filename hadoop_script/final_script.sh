#!/bin/bash

# Définir les chemins HDFS
input_path="/Input"
tmp_path="/data_tmp"

# Chemin du script Spark
spark_script="/home/sylvain/IdeaProjects/Tesr_spark/src/scala/Main.scala"

# 1. Vérification si le dépôt input est vide
echo "Vérification du dépôt input sur HDFS..."
hdfs dfs -count -e $input_path
if [ $? -eq 0 ]; then
    echo "Le dépôt input sur HDFS est vide."
else
    echo "Le dépôt input sur HDFS n'est pas vide."
fi

# Lancer le script pour copier les fichiers dans le depot
echo "Copie des fichiers dans le dépôt input sur HDFS..."
./copie_data_input.sh

#Lancer le script pour copier les fichiers dans le dossier temporaire
echo "Copie des fichiers dans le dossier temporaire sur HDFS..."
./copie_data_tmp.sh

# Lancer le script pour l' Application Spark
echo "Lancement de l'application Spark..."
./run_spark.sh

# lancer le script pour la  Suppression du dossier temporaire
echo "Suppression du dossier temporaire sur HDFS..."
./delete_tmp.sh

# lancer le script pour la  Suppression du contenu du dépôt input
echo "Suppression du contenu du dépôt input sur HDFS..."
./delete_content_input.sh

echo "Script terminé."
