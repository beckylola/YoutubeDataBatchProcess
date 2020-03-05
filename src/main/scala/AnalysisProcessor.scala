import org.apache.spark.sql.functions.{desc, sum}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object AnalysisProcessor {
  def getTop100VideosWithHighestView(): Unit = {
    val sourcePath = "hdfs://localhost:9000/youtube_processed/finalDataSet.csv"
    val destPath = "hdfs://localhost:9000/youtube_processed/top100Videos.csv"
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))
    val top100 = df2.sort(desc("views")).limit(100)
    top100.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getTop100VideosWithHighestViewPerCountry(): Unit = {
    val sourcePath = "hdfs://localhost:9000/youtube_processed/finalDataSet.csv"
    val destPath = "hdfs://localhost:9000/youtube_processed/top100VideosPerCountry.csv"
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))
    val w = Window.partitionBy("country").orderBy(desc("views"))
    val top100 = df2.withColumn("rn", row_number.over(w))
      .where("rn<=100")
      .drop("rn")
      .sort(desc("country"), desc("views"))
    top100.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getCategoriesWithViewCount(): Unit = {
    val sourcePath = "hdfs://localhost:9000/youtube_processed/finalDataSet.csv"
    val destPath = "hdfs://localhost:9000/youtube_processed/CategoriesWithViewCount.csv"
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))

    val df3 = df2.groupBy("category").agg(sum("views")).sort(desc("sum(views)")).limit(5)
    df3.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def test(): Unit = {
    val sourcePath = "hdfs://localhost:9000/youtube_processed/top100VideosPerCountry.csv"
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)

    csvDf.show(100)
    sparkSession.close()
  }
}
