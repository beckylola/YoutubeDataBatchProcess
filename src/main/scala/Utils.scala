import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Utils {
  def getSparkSession: SparkSession = {
    val conf = new SparkConf().setAppName("YoutubeDataset").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("YoutubeDataset").getOrCreate()
    sparkSession
  }
}
