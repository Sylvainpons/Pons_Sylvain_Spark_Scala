#!/bin/bash

# Définir le chemin HDFS du dossier data_tmp
tmp_path="/data_tmp"

# Supprimer le dossier data_tmp et son contenu
hadoop fs -rm -r -skipTrash $tmp_path
