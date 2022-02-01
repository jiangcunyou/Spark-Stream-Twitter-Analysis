package com.teamone.spark

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.typedLit
object loadModel extends App {
    val sparkConf = new SparkConf().setAppName("Train Naive Bayes model").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    println("Spark version = " + sc.version)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    println("Spark SQL context: " + sqlContext)
    import sqlContext.implicits._

    val model = PipelineModel.read.load("data/nbmodel")


    val seq = Seq(("2@1333331VirginAmerica yes, nearly every time I fly VX this “ear worm” won’t go away","positive"))
    val df = seq.toDF("text","senti")
//
//
    val predictions: DataFrame = model.transform(df)
    val translationMap: Column = typedLit(Map(
        0.0 -> "Late",
        2.0 -> "Bad Fligt",
        4.0 -> "Customer Service Issue",
        6.0 -> "Good"
    ))
    val rf = predictions.select($"text",$"senti",translationMap($"prediction") as "topic").show()
//    rf.coalesce(1).write
//        .format("csv")
//      .option("header","true")
//      .mode("append")
//      .save("data/csv")
//
//    rf.show()


}
