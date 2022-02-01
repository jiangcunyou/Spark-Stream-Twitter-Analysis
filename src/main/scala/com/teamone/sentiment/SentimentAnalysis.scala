package com.teamone.sentiment

import java.util.Properties
import com.teamone.sentiment.Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._



object SentimentAnalysis {

  val props = new Properties()

  // Clean data
  props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  /**
   *
   * @param input List of tweets
   * @return List of tweets & sentiments
   */
  def twAndSentiments(input: List[String]): List[(String, Sentiment)] =  input map twAndSentiment

  /**
   *
   * @param input A tweet
   * @return Tweet itself and its sentiment analysis
   */
  def twAndSentiment(input: String): (String, Sentiment) = (input, mainSentiment(input))

  /**
   *
   * @param input List of tweets
   * @return List of sentiments
   */
  def mainSentiments(input: List[String]): List[Sentiment] = input map mainSentiment

  /**
   *
   * @param input A tweet
   * @return The main sentiment for a given tweet
   */
  def mainSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if text.nonEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  /**
   * Another way to extract a list of sentiments
   */
  def sentiment(input: String): List[(String, Sentiment)] = Option(input) match {
    case Some(text) if text.nonEmpty => extractSentiments(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  /**
   * Extracts sentiment for a given input
   */
  private def extractSentiment(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  /**
   * Extracts list of texts and sentiments for a given input list
   */
  private def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

}
