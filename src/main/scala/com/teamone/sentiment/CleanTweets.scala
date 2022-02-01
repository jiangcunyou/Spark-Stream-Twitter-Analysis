package com.teamone.sentiment


object CleanTweets {

  // Data from twitter would be like "RT @User: xxx", clean it
  def clean(input: String): String = input.substring(input.indexOf(": ") + 2)

}
