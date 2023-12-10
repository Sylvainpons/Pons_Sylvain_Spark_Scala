#!/bin/bash


hdfs dfs -count -e /Input
if [ $? -eq 0 ]; then
    echo "Le dépôt input sur HDFS est vide."
else
    echo "Le dépôt input sur HDFS n'est pas vide."
fi
