package com.teamone.spark

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel, OnlineLDAOptimizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.languageFeature.implicitConversions

object LdaExample2 extends App {

  val sparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]").getOrCreate()

  val df: DataFrame = sparkSession.read.format("csv")
      .option("header","true")
      .load("data/actualdata/Tweets.csv")

  val processeddata = Preprocess.run(df,sparkSession)
  val lda_countVector = processeddata._1

//  lda_countVector.take(1)

  //-------------------------1.Training Model-----------------------------
  /**
   * K:Number of cluster centers
   * DocConcentration:The hyperparameter of the article distribution (the parameter of the Dirichlet distribution) must be >1.0.The larger the value, the smoother the inferred distribution.Default value is -1.
   * TopicConcentration:The super parameter of the topic distribution (the parameter of the Dirichlet distribution) must be >1.0. The larger the value, the smoother the inferred distribution.Default value is -1.
   * MaxIterations:Number of iterations, should be sufficient iteration, at least 20 times.
   * Optimizer:Optimization calculation method,too many iterations may not have enough memory to throw a Stack exception.
   *  */
  val lda = new LDA()
    .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
    .setOptimizer("em")
    .setK(4)
    .setMaxIterations(100)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1)// use default values
//
  val ldaModel: LDAModel = lda.run(lda_countVector)

  //--------------------------2.Model And Description---------------------
  //Describe the final pre-maxtermspertopic words (the most important word vector) and their weights for each topic
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

  //LDApridict.run(sparkSession,ldaModel)

}
