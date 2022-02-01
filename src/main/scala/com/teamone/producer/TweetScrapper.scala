package com.teamone.producer

import com.teamone.Utils.Configure
import com.teamone.elastic.ToElastic
import com.teamone.spark.{TrainingUtils, TweetUtils}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
//import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import com.teamone.sentiment.Sentiment.Sentiment
import com.teamone.sentiment.{CleanTweets, SentimentAnalysis}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, ForeachWriter, Row, SparkSession}

object TweetScrapper {
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source

    val lines = Source.fromFile("data/actualdata/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val keywords =  Configure.tweetfiltersc.getString("KEYWORDS").split(",").toSeq
    println(keywords)
    val tweets = TwitterUtils.createStream(ssc, None,keywords)
    // Now extract the text of each status update into DStreams using map()
    val statuses: DStream[String] = tweets.filter(t=>t.getLang()=="en").map(status => status.getText)


    val spark = SparkSession.builder
      .master(Configure.sparkc.getString("MASTER_URL"))
      .appName("TweetStream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._
    val model = PipelineModel.read.load("data/nbmodel")
    println("loading model")

    case class tweetsgroupsCaseClass(text: String, topic: String)

    statuses.foreachRDD( rdd => {

      rdd.collect().foreach(row => {

//        println(i)
        val out: (String, Sentiment) = SentimentAnalysis.twAndSentiment(CleanTweets.clean(row.toString))
        //2.fit logistic regression training model

        val tweet = out._1;
        val parts = tweet split(",") //space = dot operator (,) in Scala
        val texts = parts mkString(" ") //text data may contain comma, so get slice of string till end
        val text = TweetUtils.filterOnlyWords(texts)

        val seq= Seq((text,out._2.toString))

        val df: DataFrame = seq.toDF("text","airline_sentiment")
//                df.show()
        val predictions: DataFrame = model.transform(df)
//        predictions.show()
//        println(predictions.select($"text").to)
//        println(predictions.select($"prediction").toString())
        val result = predictions.select($"text",$"prediction",$"airline_sentiment")
        result.show(false)
        // Upload to Elastic
        ToElastic.dataFrameToElastic(result, "tweetsairline/doc")
//
        println("save----------------")
      })
    })

    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }


}
