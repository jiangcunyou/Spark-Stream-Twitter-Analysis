package com.teamone.spark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegressionModel extends App {
    val sparkConf = new SparkConf().setAppName("Train Naive Bayes model").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    println("Spark version = " + sc.version)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    println("Spark SQL context: " + sqlContext)
    import sqlContext.implicits._


    //load training data
    val newsgroupsRawData: RDD[String] = sc.textFile("data/actualdata/train.csv")
    //counts
    println("The number of documents read in is " + newsgroupsRawData.count() + ".")

    case class newsgroupsCaseClass(text: String, topic: String)

    //remove all other character but words
    val newsgroups: DataFrame = newsgroupsRawData.map{case (lines) =>
        val topic = lines.split(",").take(1)(0)
        val text = TrainingUtils.processText(lines)
        newsgroupsCaseClass(text,topic)}.toDF()
    newsgroups.cache()

    newsgroups.printSchema()

    newsgroups.sample(false,0.001,10L).show(10,false)

    newsgroups.groupBy("topic").count().show()

    //transform to another dataframe
    val labelednewsgroups = newsgroups.withColumn("label", newsgroups("topic").cast("double"))

    labelednewsgroups.sample(false,0.003,10L).show(5,false)

    //Split documents from a list of (id, text, label) tuples¶
    val Array(training, test) = labelednewsgroups.randomSplit(Array(0.9, 0.1), seed = 12345)
    println("test schema")
    test.show(1,false)
    test.schema.printTreeString()
//
    println("Total Document Count = " + labelednewsgroups.count())
    println("Training Count = " + training.count() + ", " + training.count*100/(labelednewsgroups.count()).toDouble + "%")
    println("Test Count = " + test.count() + ", " + test.count*100/(labelednewsgroups.count().toDouble) + "%")
//
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered").setCaseSensitive(false)
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("filtered").setOutputCol("rawFeatures")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(0)
    val lr = new LogisticRegression().setRegParam(0.01).setThreshold(0.5)
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf, lr))
//
    println("Logistic Regression Features Column = " + lr.getFeaturesCol)
    println("Logistic Regression Label Column = " + lr.getLabelCol)
    println("Logistic Regression Threshold = " + lr.getThreshold)
//
    remover.getStopWords.foreach(println)

    //Fit the pipeline to the training documents
    val model: PipelineModel = pipeline.fit(training)
//    model.save("data/nbmodel")

    //predict with test data
    val predictions: DataFrame = model.transform(test)
    val translationMap: Column = typedLit(Map(
        0.0 -> "Late issue",
        2.0 -> "Fligt experience issue",
        4.0 -> "Customer Service Issue",
        6.0 -> "Other Type"
    ))

    println("-----1-----")
//    val rf = predictions.select($"text",translationMap($"prediction") as "topic").sample(false,0.1,10L).show(5,false)
    predictions.select( "text", "prediction", "topic").sample(false,0.1,10L).show(20)
//    predictions.select("topic", "probability", "prediction", "label").sample(false,0.1,10L).show(5)
//    println("-----2-----")
//    predictions.select( "topic", "probability", "prediction", "label").filter(predictions("topic").like("comp%")).sample(false,0.1,10L).show(5)

    //Optimize
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
    println("The topic prediction accuracy of test data = " + (evaluator.evaluate(predictions)))
//
//    val paramGrid = new ParamGridBuilder().
//      addGrid(hashingTF.numFeatures, Array(1000, 10000, 100000)).
//      addGrid(idf.minDocFreq, Array(0,10, 100)).
////      addGrid(lr.regParam, Array(0.01, 0.1, 0.2)).
////      addGrid(lr.threshold, Array(0.5, 0.6, 0.7)).
//      build()
//
//    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)
//
//    val cvModel = cv.fit(training)
//    println("Area under the ROC curve for best fitted model = " + evaluator.evaluate(cvModel.transform(test)))
//
//    println("Area under the ROC curve for non-tuned model = " + evaluator.evaluate(predictions))
//    println("Area under the ROC curve for fitted model = " + evaluator.evaluate(cvModel.transform(test)))
//    println("Improvement = " + "%.2f".format((evaluator.evaluate(cvModel.transform(test)) - evaluator.evaluate(predictions)) *100 / evaluator.evaluate(predictions)) + "%")
//
//    cvModel.avgMetrics
//
//    cvModel.transform(test).select( "topic", "probability", "prediction", "label").sample(false,0.1,0L).show(5)
//    cvModel.transform(test).select( "topic", "probability", "prediction", "label").sample(false,0.1,0L).show(50)
}
