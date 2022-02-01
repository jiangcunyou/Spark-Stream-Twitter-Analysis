package com.teamone.spark

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object LdaExample extends App {
  val sparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]").getOrCreate()

  val df = sparkSession.read.format("csv")
      .option("header","true")
      .load("data/actualdata/Tweets.csv")
//
//
  print(df.show(10))

  val corpus: RDD[String] = df.rdd
    .map(row => {
        if(row.getAs("text")!=null) {
          row.getAs("text")
        }
      }.toString

    )

//
//  //打印前10个
  print(corpus.count())
  println("-----------------")
//  corpus.map(println(_)).sortBy(ascending=true).take(10)
//  corpus.zipWithIndex()
//    .sortByKey(ascending = false)
//    .take(10)
//    .map(println(_))

  //停词
  val stopWordtxt = sparkSession.read.text("data/actualdata/stopwords.txt")
    .collect().map(row => row.getString(0))

  val add_stopwords = Array("flight", "thanks", "thank", "still","flights","plane")
  val stopWords  =  stopWordtxt.union(add_stopwords).toSet
////
////
  val vocabArray: Array[String] = corpus.map(_.toLowerCase.split("\\s"))
    .map(_.filter(_.length > 3)
      .filter(_.forall(java.lang.Character.isLetter)))
    .flatMap(_.map(_ -> 1L))
    .reduceByKey(_ + _)
    .filter(word => !stopWords.contains(word._1))
    .map(_._1).collect()

  println(vocabArray.take(10).mkString(","))
//
  val tokenized: RDD[Seq[String]] =
    corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).
      filter(_.forall(java.lang.Character.isLetter)))
//  print(vocabArray(0))
val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
  // Convert documents into term count vectors
  val documents: RDD[(Long, linalg.Vector)] = tokenized.zipWithIndex.map { case (tokens, id) =>
      val counts = new mutable.HashMap[Int, Double]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val idx = vocab(term)
          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
        }
      }
      (id, Vectors.sparse(vocab.size, counts.toSeq))
    }
6
  val lda = new LDA().setK(3).setMaxIterations(20)
  val ldaMode = lda.run(documents)

  ldaMode.describeTopics(maxTermsPerTopic = 5).foreach { case (terms, termWeights) =>
    terms.zip(termWeights).foreach { case (term, weight) =>
      println(f"${vocabArray(term.toInt)} [$weight%1.2f]\t")
    }
    println()
  }

  println("------finish")
}
