package com.teamone.spark

import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable
import org.apache.spark.sql.{DataFrame, SparkSession}

object LDApridict extends App {
  def run(sparkSession:SparkSession,ldamodel:LDAModel): Unit ={

    val df: DataFrame = sparkSession.read.format("json")
      .option("header","true")
      .load("data/actualdata/smile-annotations-final.json")

    val processeddata = Preprocess.run(df,sparkSession)
    val lda_countVector = processeddata._1

//    val sameModel = DistributedLDAModel.load(sparkSession.sparkContext,path)
//    val localLDAModel = ldamodel

    //create test input, convert to term count, and get its topic distribution


      val topicDistributions: RDD[(Long, Vector)] = ldamodel.asInstanceOf[DistributedLDAModel].toLocal.topicDistributions(lda_countVector)
    println("-------------")
      println("first topic distribution:"+topicDistributions.first._2.toArray.mkString(", "))
    topicDistributions.take(10).foreach(println)

      val topicIndices = ldamodel.describeTopics(maxTermsPerTopic = 5)
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


  }


}
