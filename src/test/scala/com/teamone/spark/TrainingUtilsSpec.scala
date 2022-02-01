package com.teamone.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkConf, SparkContext}
import java.io.InputStream

class TrainingUtilsSpec extends AnyFlatSpec with should.Matchers {

  behavior of "toTuple"

  "toTuple" should "work" in{
    val xs = TrainingUtils.toTuple("It's a sunny day We love the flight,")
    xs._1 shouldBe "It's a sunny day We love the flight"
  }

  behavior of "processText"
  "processText" should "work" in{
    val xs = TrainingUtils.processText("sad asddfs,fa,,,, asde  ,,.asd sadexvcbcu")
    xs shouldBe " asde .asd sadexvcbcu"
  }

  behavior of "featureVectorization"

  "featureVectorization" should "work" in{
    val numFeatures = 1000
    val hashingTF = new HashingTF(numFeatures)

    val xs = TrainingUtils.featureVectorization("sdajisd,sad.sdaw sa")
    val xx = hashingTF.transform("sdajisd,sad.sdaw sa".sliding(3).toSeq)

    xs shouldEqual xx
  }


}
