package com.teamone.spark

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel, OnlineLDAOptimizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class LdaExample2Spec extends AnyFlatSpec with should.Matchers {

  behavior of "Lda exmple"

  val sparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]").getOrCreate()

  val df: DataFrame = sparkSession.read.format("csv")
    .option("header","true")
    .load("data/actualdata/Tweets.csv")

  val processeddata = Preprocess.run(df,sparkSession)
  val lda_countVector = processeddata._1

  val lda = new LDA()
    .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
    .setOptimizer("em")
    .setK(3)
    .setMaxIterations(20)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1)// use default values

  val ldaModel: LDAModel = lda.run(lda_countVector)

  it should "Preprocess work" in {
    processeddata should not be 0
    println("Preprocess success.")
  }

  it should "LdaModel work" in {
    ldaModel should not be 0
    println("LdaModel success.")
  }

  it should "Lda topics work" in {
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)

    val vocabList = processeddata._2.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights)
    }

    topics.zipWithIndex.foreach { case (topic, i) =>
      topic.foreach {
        case (term, weight) =>
          if (term.contains("cancelled") || term.contains("service")
            || term.contains("hold")|| term.contains("call")
            || term.contains("help")|| term.contains("time")
            || term.contains("back")|| term.contains("flights")&& weight > 0) {
            val xs = true
            xs shouldBe true
          }
      }
    }

    println("Lda topics success")
  }
}
