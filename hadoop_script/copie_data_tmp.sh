#!/bin/bash

# Définir les chemins HDFS
input_path="/Input"
tmp_path="/data_tmp"

# Créer le dossier data_tmp sur HDFS
hadoop fs -mkdir $tmp_path

# Copier les fichiers depuis le dépôt input vers le dossier data_tmp
hadoop fs -cp $input_path/* $tmp_path
