import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._

object TwitterAnalysis {

  def main(args: Array[String]) {
    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
        System.err.println("Usage: Demo <consumer key> <consumer secret> " +
          "<access token> <access token secret> [<filters>]")
        System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length -4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("TA").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(60))

    val checkPoint: String = "./checkpoint"
    ssc.checkpoint(checkPoint)

    //Update function for topic score
    def updateTopicScore(key: String, value: Option[Int], state: State[Int]): (String, Int) =
    {
      val currentScore: Int = state.getOption().getOrElse(0)
      val newScore = value.getOrElse(0)
      state.update(newScore)
      (key, newScore - currentScore)
    }

    // creating twitter input stream
    val tweets = TwitterUtils.createStream(ssc, None, filters)

    //tweets.print()

    // Apply the sentiment analysis using detectSentiment function in the SentimentAnalysisUtils
    tweets.foreachRDD{(rdd, time) =>
      rdd.map(t => { Map(
        "user"-> t.getUser.getScreenName,
        "text" -> t.getText,
        "hashtags" -> t.getHashtagEntities.map(_.getText),
        "language" -> t.getText,
        "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString )
      }).filter(v => {
        val tweetArray = v("hashtags").asInstanceOf[Array[String]]
        tweetArray.length > 0 && v("language").equals("en")
      })
    }

    // define StateSpec for mapWithState
    val stateSpec = StateSpec.function(updateTopicScore _)

    val windowTweets = tweets.transform(rdd => {
      rdd.map(t => {
        Map(
          "user"-> t.getUser.getScreenName,
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "language" -> t.getLang,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString)
      })
    }).filter(v => {
      val tweetArray = v("hashtags").asInstanceOf[Array[String]]
      tweetArray.length > 0 && v("language").equals("en")
    }).window(Seconds(60), Seconds(60))

    // Calculate the emerging topics
    val topics = windowTweets.flatMap(t => t("hashtags").asInstanceOf[Array[String]])
      .map(h => (h, 1))
      .reduceByKey(_+_)
      .mapWithState(stateSpec)
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // From the topics d stream, extract the emerging topic
    var eTopic = ""
    topics.foreachRDD(rdd => {
      val top = rdd.take(1)
      top.foreach{case (count, topic) => eTopic = topic}
    })

    // Print all the tweets in the window with the emerging topic
    windowTweets.foreachRDD(rdd => {
      println("Emerging topic: %s".format(eTopic))
      rdd.filter(t => {
        val hashtagList = t("hashtags").asInstanceOf[Array[String]]
        hashtagList.contains(eTopic)
      }).repartition(1).saveAsTextFile("./outputFileFor_"+eTopic)
    })

    ssc.start() // Start streaming
    ssc.awaitTermination() // Wait for Termination
  }
}
