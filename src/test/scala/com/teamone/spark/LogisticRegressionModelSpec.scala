package com.teamone.spark

import com.teamone.spark.TrainingUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

class LogisticRegressionModelSpec extends AnyFlatSpec with should.Matchers {

  behavior of "NavieBayes"

  val sparkConf = new SparkConf().setAppName("Train model").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
  println("Spark version = " + sc.version)
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  println("Spark SQL context: " + sqlContext)
  import sqlContext.implicits._

  val newsgroupsRawData: RDD[String] = sc.textFile("data/actualdata/train.csv")

  println("The number of documents read in is " + newsgroupsRawData.count() + ".")

  case class newsgroupsCaseClass(text: String, topic: String)

  it should "Read data work" in {

    newsgroupsRawData.count() shouldBe 7446
    println("Read Data Success.")
  }

  it should "News groups work" in{
    val x = newsgroupsCaseClass("An easy test.","positive")
    x should matchPattern{
      case newsgroupsCaseClass("An easy test.","positive") =>
    }
    println("News group Success.")
  }

}
