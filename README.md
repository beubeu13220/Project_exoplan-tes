# Projet exoplanètes


#### Commande pour lancer le script du Job sur les TP 2/3

./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.Job /home/brehelin/Téléchargements/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar


#### Commande pour lancer le script du Job sur les TP 4/5 

./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.jobML1 /home/brehelin/Téléchargements/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar

#### Remarques

> L'exécution du job sur les TP 4/5 permet en plus des taches demandées, d'afficher une courbe ROC sur nos données. Celle-ci est générée gràce au package **wisp** (implémentation de HighCharts API) que l'on "load" dans notre library avec la commande : *"com.quantifind" %% "wisp" % "0.0.4"* (insérée dans le fichier built.sbt)
Une fois le job lancé, la courbe s'affichera dans le navigateur internet par défaut. 
De plus, cette exécution génére un fichier .html dans le dossier où vous exécutez la commande ./spark-submit.


> Le script du job sur les TP 4/5 contient une variable *path* pour définir un chemin générique pour l'exéctution du job (où trouver la base de donnée et où sauvegarder le modèle)



> Le script du job sur les TP 2/3 ne contient pas de variable *path* 
 

