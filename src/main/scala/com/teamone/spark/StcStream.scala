package com.teamone.spark


import com.teamone.Utils.Configure
import com.teamone.sentiment.Sentiment.Sentiment
import com.teamone.sentiment.{CleanTweets, SentimentAnalysis}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StcStream extends App {

  val spark = SparkSession.builder
    .master(Configure.sparkc.getString("MASTER_URL"))
    .appName("TweetStream")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._

  val ds: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "TwitterData2") //
    .load()

  val selectds: DataFrame = ds.selectExpr("CAST(value AS STRING)")

//  selectds.printSchema()

    //nbmodel
    val model = PipelineModel.read.load("data/nbmodel")
    println("载入model")
    val translationMap1: Column = typedLit(Map(
      0.0-> "Late",
      2.0-> "Bad Fligt",
      4.0-> "Customer Service Issue",
      6.0-> "Good "
    ))
    //nb model end

  val write1 = selectds.writeStream
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//      batchDF.persist()
      val b= batchDF.foreach({row=>
        //get sentiment result
        val out: (String, Sentiment) = SentimentAnalysis.twAndSentiment(CleanTweets.clean(row.toString))
        //2.fit navie bayes training model
        val seq= Seq((out._1,out._2.toString))
        println(seq.toString())
        val df = seq.toDF("text","airline_sentiment")
        df.show()
//
//        //could work above ,but can't write to file...
//        val predictions: DataFrame = model.transform(df)
//        val result = predictions.select($"text",$"airline_sentiment")
//          .write.format("csv")
//          .save("data/csv")
      })
    }
//    .outputMode("update")
    .start()

  write1.awaitTermination()


//
//  val customwriter: ForeachWriter[Row] = new ForeachWriter[Row] {
//    val drive = "com.mysql.jdbc.Driver"
//    var connection:Connection = _
//    var statement:Statement = _
//    def open(partitionId: Long, version: Long): Boolean = {
//      Class.forName(drive)
//      connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/db1?characterEncoding=UTF-8","root","123456")
//      statement = connection.createStatement()
//      true
//    }
//    def process(record: Row): Unit = { //tweet:String
//      // Write string to connection
////      MongoDBConnection.insert(record(0).toString())
//      println(record(0))
//      //1.情感分析
//      val out: (String, Sentiment) = SentimentAnalysis.twAndSentiment(CleanTweets.clean(record(0).toString))
//      //2.nbmodel
//
//      val seq= Seq((out._1,out._2.toString))
//      val df = seq.toDF("text","airline_sentiment")
////      val predictions: DataFrame = model.transform(df)
////      val result = predictions.select($"text",$"airline_sentiment",translationMap1($"prediction") as "topic").rdd
//      df.show()
////      statement.executeUpdate("INSERT INTO spark "+"VALUES ("+result+")")
//    }
//    def close(errorOrNull: Throwable): Unit = {
//      Unit
//    }
//  }
////
//  val writedf = selectds.writeStream
//      .outputMode("update")
//  .trigger(Trigger.Continuous("2 seconds"))
//    .foreach(customwriter)
//    .start()
////
//  writedf.awaitTermination()
}
