package com.teamone.producer

import java.util.Properties

import com.teamone.Utils.Configure
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.twitter.TwitterUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterObjectFactory, TwitterStreamFactory}

object TweetScrapperWithKafka {
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  def setupTwitter(): Unit = {
    import scala.io.Source

    val lines = Source.fromFile("data/actualdata/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }


  def main(args: Array[String]) ={
    // set log level
    setupLogging()


    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(Configure.twitter.getString("CONSUMER_KEY"))
      .setOAuthConsumerSecret(Configure.twitter.getString("CONSUMER_KEY_SECRET"))
      .setOAuthAccessToken(Configure.twitter.getString("ACCESS_TOKEN"))
      .setOAuthAccessTokenSecret(Configure.twitter.getString("ACCESS_TOKEN_SECRET"))
      .setJSONStoreEnabled(true)


    //create kafka props
    val props = new Properties()
    props.put("bootstrap.servers", Configure.kafkac.getString("BOOTSTRAP_SERVERS"));
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // StringSerializer encoding defaults to UTF8
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // StringSerializer encoding defaults to UTF8

    val producer = new KafkaProducer[String, String](props)
    val kafkatopic = Configure.kafkac.getString("TOPIC")
    val statuslistener = new StatusListener {
      /*
      StatusListener defines what to do with the tweets as they stream
      */
      def onStatus(status:Status) {
        println(status.getText)
        val data = new ProducerRecord[String, String](kafkatopic, null, status.getText)
        producer.send(data) // (topic,key,value) //TwitterObjectFactory.getRawJSON(status)
      }
      def onDeletionNotice(statusDeletionNotice:StatusDeletionNotice) {
        println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId())
      }
      def onScrubGeo(userId:Long, upToStatusId:Long) {
        println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId)
      }
      def onStallWarning(warning:StallWarning) {
        println("Got stall warning:" + warning)
      }
      def onTrackLimitationNotice(numberOfLimitedStatuses:Int) {
        println("Got track limitation notice:" + numberOfLimitedStatuses)
      }
      def onException(ex:Exception) {
        ex.printStackTrace()
      }
    }

    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
    twitterStream.addListener(statuslistener)

    val keywords:String =  Configure.tweetfiltersc.getString("KEYWORDS")
    val languages:String = Configure.tweetfiltersc.getString("LANGUAGES")

    val query = new FilterQuery().track(keywords)
      .language(languages)

//    def stream() {
      twitterStream.filter(query)
      // twitterStream.cleanUp()
      // twitterStream.shutdown()
//    }
  }


}
