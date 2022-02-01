package com.teamone.Utils

import com.typesafe.config.{Config, ConfigFactory}

object Configure {
  def twitter = ConfigFactory.load().getConfig("twitter")

  def kafkac =  ConfigFactory.load().getConfig("kafka")

  def tweetfiltersc = ConfigFactory.load().getConfig("tweetfilters")

  def sparkc = ConfigFactory.load().getConfig("spark")
}
