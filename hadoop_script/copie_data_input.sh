#!/bin/bash

# Définir les chemins HDFS
input_path="/Input"
data_path="/home/sylvain/IdeaProjects/Tesr_spark/src/main/resources"

# Copier les fichiers depuis le répertoire local vers le dépôt HDFS
hadoop fs -copyFromLocal $data_path/* $input_path
