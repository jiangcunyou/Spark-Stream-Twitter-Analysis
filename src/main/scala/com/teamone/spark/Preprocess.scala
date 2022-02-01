package com.teamone.spark

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Preprocess {
  def run(df:DataFrame,sparkSession:SparkSession): (RDD[(Long, linalg.Vector)], CountVectorizerModel) ={
    print(df.show(10))

    val corpus: RDD[String] = df.rdd
      .map(row => {
        if(row.getAs("text")!=null) {
          row.getAs("text")
        }
      }.toString.toLowerCase

      )

    print(corpus.count())
    println("-----------------")
    //  corpus.map(println(_)).sortBy(ascending=true).take(10)
    //  corpus.zipWithIndex()
    //    .sortByKey(ascending = false)
    //    .take(10)
    //    .map(println(_))
    val corpus_df: DataFrame = sparkSession.createDataFrame(corpus.zipWithIndex()).toDF("corpus", "id")

    //Tokenizing using the RegexTokenizer
    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setMinTokenLength(4)
      .setInputCol("corpus")
      .setOutputCol("tokens")

    val tokenized_df: DataFrame = tokenizer.transform(corpus_df)

    tokenized_df.select("tokens").show(10,false)

    //Removing the Stop-words using the Stop Words remover
    val add_stopwords  = Array("http","jetblue","southwestair","americanair","flight",
      "usairways","thanks", "virginamerica","thank","today","flightled","united","please")
    val stopwords = sparkSession.read.text("data/actualdata/stopwords.txt")
      .collect().map(row => row.getString(0)).union(add_stopwords)

    val remover = new StopWordsRemover()
      .setStopWords(stopwords) // This parameter is optional
      .setInputCol("tokens")
      .setOutputCol("filtered")

    val filtered_df: DataFrame = remover.transform(tokenized_df)

    //Converting the Tokens into the CountVector
    val vectorizer: CountVectorizerModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(10000)
      .setMinDF(5)
      .fit(filtered_df)

    val countVectors: DataFrame= vectorizer.transform(filtered_df).select("id", "features")
    countVectors.show(5,false)
    import sparkSession.implicits._
    val lda_countVector: RDD[(Long, linalg.Vector)] = countVectors.rdd.map {
      case Row(id: Long, countVector: Vector) => (id, Vectors.fromML(countVector)) }
    (lda_countVector,vectorizer)
  }
}
