#!/bin/bash

# Définir le chemin HDFS du dépôt input
input_path="/Input"

# Supprimer le contenu du dépôt input
hadoop fs -rm -r -skipTrash $input_path/*
