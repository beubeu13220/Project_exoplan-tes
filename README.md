# Projet exoplanètes
Spark project 

#### Commande pour lancer le script du Job sur les TP 2/3

./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.Job /home/brehelin/Téléchargements/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar


#### Commande pour lancer le script du Job sur les TP 4-5 

./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.jobML1 /home/brehelin/Téléchargements/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar

