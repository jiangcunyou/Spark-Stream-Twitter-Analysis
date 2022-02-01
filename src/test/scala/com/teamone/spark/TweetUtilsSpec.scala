package com.teamone.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers

class TweetUtilsSpec extends AnyFlatSpec with should.Matchers {

  behavior of "filter"

  "filterUnknownLocation" should "be true" in{
    val xs = TweetUtils.filterUnknownLocation("abcd")
    xs shouldBe true
  }

  "filterUnorganizedLocation" should "be true" in{
    val xs = TweetUtils.filterUnorganizedLocation("sadas,sda")
    xs shouldBe true
  }

  "filterOnlyWords" should "be true" in{
    val xs = TweetUtils.filterOnlyWords("car dreamgoodjob long o1ne t.2o two ,, w alk,, walk")
    xs shouldEqual " car dreamgoodjob long o1ne t.2o two w walk"
  }

  "filterEmptyString" should "be true" in{
    val xs = TweetUtils.filterEmptyString("asd d sa  ")
    xs shouldBe true
  }
}
