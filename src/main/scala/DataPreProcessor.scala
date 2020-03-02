import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataPreProcessor {

   def preProcessOriginalDataset() = {
    val countryList = Array("CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US")
    val sourceDataDir = "src/main/dataset/"
    //  val sourceDataDir = "hdfs://localhost:9000/youbute_kaggle/"
    val destPath = "hdfs://localhost:9000/youtube_processed/finalDataSet.csv"

    val sparkSession = getSparkSession
    val finalResult = countryList.map(country => getDatasetWithCategoryDescDf(country, sourceDataDir, sparkSession))
      .reduce(_.union(_))
    finalResult
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
  }

  private def getSparkSession: SparkSession = {
    val conf = new SparkConf().setAppName("YoutubeDataset").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("YoutubeDataset").getOrCreate()
    sparkSession
  }

  private def getDatasetWithCategoryDescDf(country: String, dataDir: String, sparkSession: SparkSession) = {
    val jsonDf: DataFrame = sparkSession.read.option("multiline", "true").json(dataDir + country + "_category_id.json")
    val categoryDf = jsonDf.select(explode(jsonDf("items"))).select("col.id", "col.snippet.title").withColumnRenamed("title", "category")
    val csvDf = sparkSession.read.option("header", "true").csv(dataDir + country + "videos.csv")
    val joinedDf = categoryDf.join(csvDf, col("id") === col("category_id"), "inner").withColumn("country", lit(country))
    val finalDf = joinedDf.select("country", "trending_date", "title",
      "channel_title", "category",
      "publish_time", "tags",
      "views", "likes",
      "dislikes", "comment_count",
      "description")
    finalDf
  }
}


