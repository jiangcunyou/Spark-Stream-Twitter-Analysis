package com.teamone.elastic

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileWriter


/**
 * Several methods to upload data to Elastic Search
 */
object ToElastic {


  /**
   * Upload the data processed to Elastic Search
   * @param dataFrame Data whose type is dataframe
   * @param path Elastic Search path
   */
  def dataFrameToElastic(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .format("org.elasticsearch.spark.sql")
      .option("es.port", 9200)
      .option("es.nodes", "localhost")
      .mode("append")
      .save(path)
  }

  /**
   * Save the data processed to local CSV file
   * @param dataFrame Data whose type is dataframe
   * @param fileName Local CSV file path
   */
  def dataframeToCSV(dataFrame: DataFrame, fileName: String): Unit = {
    val ColumnSeparator = ","
    val writer = new FileWriter(fileName, true)
    val seq: Seq[Any] = dataFrame.collect.map(_.toSeq).apply(0)
    try{
      writer.write(s"${seq.map(_.toString).mkString(ColumnSeparator)}\n")
    } finally {
      writer.flush()
      writer.close()
    }
  }

  /**
   * Upload data in local CSV file to Elastic Search
   * @param fileName Local CSV file path
   * @param path Elastic Search path
   */
  def csvToElastic(fileName: String, path: String): Unit = {
    val sc = SparkSession.builder().appName("WriteToElastic").master("local[*]").getOrCreate()

    val lines: DataFrame = sc.read.option("header", "true").csv("data/actualdata/ElasticTest.csv")

    // lines.show()

    dataFrameToElastic(lines, path)
  }

}
