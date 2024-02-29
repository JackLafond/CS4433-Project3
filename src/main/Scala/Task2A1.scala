import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession


object Task2A1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Task2A1")

    val csvFilePath = "D:\\IntellijProjects\\CS4433-Project3\\data_problem2\\purchases.csv"

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    //df.show()

    df.createOrReplaceTempView("purchase_table")

    val Table1 = spark.sql("SELECT * FROM purchase_table WHERE TransTotal < 600")

    Table1.show()

    spark.stop()
  }

}
