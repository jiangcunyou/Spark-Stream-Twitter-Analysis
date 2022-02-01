Spark-stream-twitter-analysis
![GitHub top language](https://img.shields.io/github/languages/top/FanWu6/Spark-stream-twitter-analysis.svg)
==============
Final project of CSYE-7200

Use cases
--------------------
User inputs the name of an airplane company and receives a comprehensive analysis on public opinion.

## Configuration

1.Apply twitter developer account, get consumerKey, consumerSecret, accessToken,accessTokenSecret  and write them into "data/actualdata/twitter.txt"

2.Change "tweetfilters" keywords in "resources/application.conf" to fullfill your object.

Usage
------------------
At first, you need to run `LogisticRegressionModel.scala` under `package com.teamone.spark`, which will help to build and train the model.

After installing Elastic search and Kibana, you only need to change the filter keyword(Airline company name) in `application.conf` under `resources` directory and run the `TweetScrapper.scala` under `com.teamone.producer`. And then you can see the result at http://localhost:5601.

Details
-------------------------------------
![](https://github.com/FanWu6/Spark-stream-twitter-analysis/blob/main/UseCases.png)

Our model is based on Logistic Regression, and the topics are found by Latent Dirichlet Allocation.

There are two twitter scrapers: one with spark-streaming and the other with Kafka.

Sentiment analysis is imported from [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/).

Data will be updated to http://localhost:9200, and people can use [Kibana](http://localhost:5601) to see and manage the data.

If you want to user Twitter with kafka, you need to lauch zookeeper and kafka service first.

Contributors
-----------------------------
[Fan Wu](https://github.com/FanWu6),
[Dayu Jia](https://github.com/Tutfa),
[Bowen Jiang](https://github.com/jiangcunyou)

License
----------------
[MIT LICENSE](../LICENSE)
