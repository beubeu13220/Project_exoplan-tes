package com.sparkProject

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import com.quantifind.charts.Highcharts._
import org.apache.spark.sql.types.{StructType}
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.DenseVector

import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
/**
  * Created by brehelin on 27/10/16.
  *
  *
  */
/*
Ligne de code pour submit le programme scala
./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.jobML1 /home/brehelin/Téléchargements/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar
*/

object jobML1 {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      .appName("spark session TP_parisTech")
      .getOrCreate()

    //Chemin du fichier avec la base nettoyée
    val path = "/home/brehelin/Téléchargements/cleanedDataFrame/"

    val sc = spark.sparkContext

    //Import de notre clean data
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").option("comment", "#")
      .csv(path + "ma_base.csv")

    //On crée nos deux dataframes de features et de label
    val features = df.drop("rowid", "koi_disposition")
    val labels = df.select("koi_disposition")

    //On définit une transformation sous le nom assembler qui renvoie nos features en ligne
    val assembler = new VectorAssembler()
      .setInputCols(features.columns.map(cname => s"${cname}"))
      .setOutputCol("features")

    //On applique la transformation sur le dataframe features
    val output = assembler.transform(features).select("features")

    //Ici il s'agit de la méthode pour centré-réduire
    //Elle n'est pas exécuté ici car nous efféctuons cette opération lors de l'ajustement du modèle
    /*val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(output)
    val scaledData = scalerModel.transform(output).select("scaledFeatures")*/


    //On définit une transformation sur le dataframe de label pour conserver un label sous la forme binaire
    val indexer = new StringIndexer()
      .setInputCol("koi_disposition")
      .setOutputCol("label")

    //On applique la transformation indexer sur nos données et on conserve seulement les données transformées
    val indexed = indexer.fit(labels).transform(labels).select("label")

    //On merge les dataframes indexed(=label) et output(=features)

    //Etape 1 : on merge les lignes
    val rows = output.rdd.zip(indexed.rdd).map{
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)}

    //Etape 2 : on merge les schemas
    val schema = StructType(output.schema.fields ++ indexed.schema.fields)

    //Etape 3 : on crée un nouveau dataframe à partir des deux merge précédent
    val dataDF: DataFrame = sqlContext.createDataFrame(rows, schema)


    //On split le dataframe en training (60%) et test (40%).
    val splits = dataDF.randomSplit(Array(0.9, 0.1), seed=1)
    val training = splits(0)
    val test = splits(1)

    //Affichage du schema de la table d'apprentissage
    training.printSchema()


    //Définition de notre modèle de prédiction
    val lr = new LogisticRegression()
      .setElasticNetParam(1.0)  // L1-norm régularisation: LASSO
      .setLabelCol("label")
      .setStandardization(true)  // On standardize nos données
      .setFitIntercept(true)  // On ajustera une constante dans notre modèle
      .setTol(1.0e-5)  // Critère de convergence
      .setMaxIter(300)  // Nombre d'itération maximale

    //On crée ici notre grille de paramètre

    //val array = -6.0 to (0.0, 0.5) toArray
    //val arrayLog = array.map(x => math.pow(10,x))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(1,10e-6))
      .build()


    //On implémente ici la méthode de cross validation sur k=3 et
    //on utilise la classe BinaryClassificationEvaluator (aire sous la courbe ROC) pour mesurer la
    // performance moyenne sur chaque folds
    //le meilleur paramètre pour regParam sera le modèle qui renvoi la meilleur performance moyenne
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1)

    //Ci-dessous le code pour une trainvalidation qui effectue 1 validation sur 70%vs20%
    //comme demandé dans l'énoncé
    /*
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 70% des données pour entrainer le modèle puis 20% pour le valider
      .setTrainRatio(0.7)
     */

    //On ajuste le modèle que nous avons cross-validé auparavant
    val cvModel = cv.fit(training)

    //Le modèle est sauvegarder dans le chemin défini au début du code
    cvModel.write.overwrite().save(path+"save_best_model")

    //On renvoit la performance de notre modèle du point de vu prédictif
    val test_metric = cvModel.avgMetrics
    //La metric par défaut est l'aire sous la courbe ROC
    println {
      test_metric.mkString(" ")
    }

    //Permet d'afficher les paramètres optimaux du modèle ajusté
    val lrModel = cvModel.bestModel.extractParamMap()
    println(lrModel)

    //On applique le modèle prédicit sur l'échantillon test
    val df_with_prediction = cvModel.transform(test)
      .select("label", "prediction","probability")

    //Ici on affiche la matrice de confusion de notre prédiction
    df_with_prediction.groupBy("label", "prediction").count.show()

    //On crée un nouveau classifier pour mesurer la performance de notre modèle sur l'échantillon test
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

    //On renvoit l'air sous la courbe roc pour l'échantillon test
    println(binaryClassificationEvaluator.evaluate(df_with_prediction))

    // ************************ ROC CURVE Ploting Section ************************ //

    //On crée un udf que l'on va utiliser pour conserver les probabilités d'appartenir à la classe 1
    //car la variable probability renvoit un vecteur de probabilité pour les classses 0 & 1
    val getPOne = udf{ v: DenseVector => v(1)}

    val df_with_proba = cvModel.transform(test).select("label","probability")

    val df_with_proba_2 = df_with_proba.withColumn("new_prob", getPOne(df_with_proba("probability")))

    //On transforme nos données de probabilité et de label sous un RDD pour pouvoir utiliser le
    //classifier binaire de MLlib
    val PredsandLabel = df_with_proba_2.select("new_prob", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val toto = new BinaryClassificationMetrics(PredsandLabel)

    //On utilise notre classifier pour collecter les données de la courbe roc de notre estimation
    val roc_curve = toto.roc().collect()

    //On crée 4 listes qui vont être utilisées pour notre plot
    //Les list1 et list2 représente les (x,y) de notre courbe roc
    val list1 = new ListBuffer[Double]()
    val list2 = new ListBuffer[Double]()
    roc_curve.foreach{case (t,p) => list1.+=(t*100)}
    roc_curve.foreach{case (t,p) => list2.+=(p*100)}

    //les listes droite et droite2 représente les (x,y) d'une droite linéaire qui représente la prédiction aléatoire
    val droite =List.range(0, 101,10)
    val droite2 = List.range(0, 101,10)

    //Utilisation du module Highcharts et de la fonction araspline pour plot nos différentes données
    areaspline(list1,list2)
    hold
    areaspline(droite,droite2)

    title("ROC Curve")
    xAxis("false positive rate")
    yAxis("true positive rate")
    legend(List("Prediction","Random"))


  }
}



