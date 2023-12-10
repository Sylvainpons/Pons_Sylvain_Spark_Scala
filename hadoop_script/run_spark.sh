#!/bin/bash

# Définir le chemin du script Spark
spark_script="/home/sylvain/IdeaProjects/Tesr_spark/src/scala/Main.scala"

# Lancer l'application Spark
spark-submit --class main.Main --master yarn --deploy-mode client $spark_script
