package com.sparkProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext


object Job {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      .appName("spark session TP_parisTech")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._


    /********************************************************************************
      *
      *        TP 1
      *
      *        - Set environment, InteliJ, submit jobs to Spark
      *        - Load local unstructured data
      *        - Word count , Map Reduce
      *
      *        ATTENTION AU CHEMIN DANS CE CODE
      ********************************************************************************/



    // ----------------- word count ------------------------

    val df_wordCount = sc.textFile("/home/brehelin/Téléchargements/spark-2.0.1-bin-hadoop2.7/README.md")
      .flatMap{case (line: String) => line.split(" ")}
      .map{case (word: String) => (word, 1)}
      .reduceByKey{case (i: Int, j: Int) => i + j}
      .toDF("word", "count")
    df_wordCount.orderBy($"count".desc).show()


    /********************************************************************************
      *
      *        TP 2 : début du projet
      *
      ********************************************************************************/

    /* #########     ###########       Partie  3         #########          ######## */
    /* 3A*/
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").option("comment","#")
      .csv("/home/brehelin/Téléchargements/cumulative.csv")
    /*3B*/
    /*Comptage du nombre de ligne*/
    println(df.count())
    /*Comptage du nombre de colonne*/
    println(df.columns.length)

    /*3C*/
    /*Affichage de la table */
    df.show()

    /*3D*/
    /* Afficher 10 a 20 colonnes de notre table*/
    /* On montre les 50 premières lignes */
    df.select(df.columns.slice(10,20).map(col) : _*).show(50)

    /*3e*/
    /*On print le schema de donnée*/
    df.printSchema()

    /*3f*/
    /*On veut le nombre d'élement de chaque classe sur la colonne koidisposition*/
    df.groupBy("koi_disposition").count().show()

    /* #########     ###########       Partie  4         #########          ######## */
    /* 4A : cleaning de donnée ,Conserver uniquement les lignes qui nous
    intéressent pour le modèle (koi_disposition = CONFIRMED ou FALSE POSITIVE )*/
    val df_2 = df.filter(df("koi_disposition")==="CONFIRMED" || df("koi_disposition")==="FALSE POSITIVE")

    /*4B : Afficher le nombre d’éléments distincts dans la colonne “koi_eccen_err1”.
    Certaines colonnes sont complètement vides: elles ne servent à rien pour l’entraînement du modèle.*/

    df_2.groupBy("koi_eccen_err1").count().show()

    /* 4C : Enlever la colonne “koi_eccen_err1”.*/

    val df_3 = df_2.drop("koi_eccen_err1")

    /* 4d */

    val list_col = List ("index","kepid","koi_fpflag_nt", "koi_fpflag_ss","koi_eccen_err1",
                    "koi_fpflag_co", "koi_fpflag_ec","koi_sparprov", "koi_trans_mod",
                    "koi_datalink_dvr", "koi_datalink_dvs", "koi_tce_delivname", "koi_parm_prov",
                      "koi_limbdark_mod", "koi_fittype", "koi_disp_prov", "koi_comment" ,
                    "kepoi_name", "kepler_name", "koi_vet_date", "koi_pdisposition")

    val df_4 = df_2.drop(list_col:_*)
    df_4.printSchema()

    /*4e*/
    /*ON veut connaitre les colonnes n'ayant qu'un seul élement*/


    val useless = for(col <- df_4.columns if df_4.select(col).distinct().count() == 1 ) yield col
    val df_5 = df_4.drop(useless:_*)
    println(df_5.columns.length)

    /*4f*/

    df_5.describe("koi_time0_err1").show()

    /*4g*/

    val df_6 = df_5.na.fill(df_5.columns.map(c => (c, 0)).toMap)

    /*5*/

    val df_labels = df_6.select("rowid", "koi_disposition")
    val df_features = df_6.drop("koi_disposition")

    val df_8 = df_features
      .join(df_labels, usingColumn = "rowid")

    /*6*/


    def udf_sum = udf((col1: Double, col2: Double) => col1 + col2)


    val df_newFeatures = df_8
      .withColumn("koi_ror_min", udf_sum($"koi_ror", $"koi_ror_err2"))
      .withColumn("koi_ror_max", $"koi_ror" + $"koi_ror_err1")



    /* export de la base*/
    df_newFeatures
        .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("/home/brehelin/Téléchargements/cleanedDataFrame.csv")



  }



}
