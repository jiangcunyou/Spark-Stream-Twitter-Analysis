package com.teamone.sentiment

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers

class SentimentAnalysisSpec extends AnyFlatSpec with should.Matchers {

  behavior of "Sentimental analysis"

  it should "work1" in {
    val input = "Scala is a great general purpose language"
    val sentiment = SentimentAnalysis.mainSentiment(input)
    sentiment should be(Sentiment.POSITIVE)
  }

    it should "work2" in {
      val input = "Dhoni laments bowling, fielding errors in series loss"
      val sentiment = SentimentAnalysis.mainSentiment(input)
      sentiment should be(Sentiment.NEGATIVE)
    }

    it should "work3" in {
      val input = "I am reading a book"
      val sentiment = SentimentAnalysis.mainSentiment(input)
      sentiment should be(Sentiment.NEUTRAL)
    }
}
