package com.teamone.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel, OnlineLDAOptimizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector, Vectors}


class PreprocessSpec extends AnyFlatSpec with should.Matchers {

  behavior of "Preprocess"
  val sparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]").getOrCreate()

  val df: DataFrame = sparkSession.read.format("csv")
    .option("header","true")
    .load("data/actualdata/Tweets.csv")

  val processeddata = Preprocess.run(df,sparkSession)
  val lda_countVector = processeddata._1
  //  lda_countVector.take(1)

  val lda = new LDA()
    .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
    .setOptimizer("em")
    .setK(3)
    .setMaxIterations(20)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1)// use default values
  //
  val ldaModel: LDAModel = lda.run(lda_countVector)
  //
  val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
  val vocabList = processeddata._2.vocabulary
  val topics = topicIndices.map { case (terms, termWeights) =>
    terms.map(vocabList(_)).zip(termWeights)
  }
  //  println(s"$numTopics topics:")
  topics.zipWithIndex.foreach { case (topic, i) =>
    println(s"TOPIC $i")
    topic.foreach { case (term, weight) => println(s"$term\t$weight") }
    println(s"==========")
  }

  def run(sparkSession:SparkSession,ldamodel:LDAModel): Boolean = {

    var xs = false
    val df: DataFrame = sparkSession.read.format("json")
      .option("header", "true")
      .load("data/actualdata/smile-annotations-final.json")

    val processeddata = Preprocess.run(df, sparkSession)
    val lda_countVector = processeddata._1

    //    val sameModel = DistributedLDAModel.load(sparkSession.sparkContext,path)
    //    val localLDAModel = ldamodel

    //create test input, convert to term count, and get its topic distribution


    val topicDistributions: RDD[(Long, Vector)] = ldamodel.asInstanceOf[DistributedLDAModel].toLocal.topicDistributions(lda_countVector)
    println("-------------")
    println("first topic distribution:" + topicDistributions.first._2.toArray.mkString(", "))
    topicDistributions.take(10).foreach(println)

    val topicIndices = ldamodel.describeTopics(maxTermsPerTopic = 5)
    val vocabList = processeddata._2.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights)
    }
    //  println(s"$numTopics topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      topic.foreach {
        case (term, weight) =>
          if (term.contains("nationalgallery") || term.contains("tateliverpool")
            || term.contains("work")|| term.contains("https")
            || term.contains("britishmuseum")|| term.contains("london")
            || term.contains("exhibition")|| term.contains("visit")&& weight > 0) {

            xs = true
          }
      }
    }
    xs
  }

  it should "Preprocess work" in {

    val xs = run(sparkSession,ldaModel)
    xs shouldBe true
    println("Preprocess success.")
  }
}
