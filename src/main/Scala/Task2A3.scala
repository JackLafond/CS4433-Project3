import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession

object Task2A3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Task2A3")

    val csvFilePath1 = "D:\\IntellijProjects\\CS4433-Project3\\data_problem2\\purchases.csv"
    val csvFilePath2 = "D:\\IntellijProjects\\CS4433-Project3\\data_problem2\\customers.csv"

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val dfP = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath1)

    val dfC = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath2)

    dfP.createOrReplaceTempView("purchase_table")

    dfC.createOrReplaceTempView("customer_table")

    val Table1 = spark.sql("SELECT * FROM purchase_table WHERE TransTotal < 600")

    Table1.createOrReplaceTempView("filtered_purchase_table")

    val Table2 = spark.sql("SELECT * FROM filtered_purchase_table INNER JOIN customer_table ON filtered_purchase_table.CustID = customer_table.ID")

    Table2.createOrReplaceTempView("joined_table")

    val Table3 = spark.sql("SELECT CustID, ANY_VALUE(Age) AS Age, SUM(TransNumItems) AS TransNumItemsSum, ROUND(SUM(TransTotal), 2) AS TransTotalSum FROM joined_table WHERE Age < 25 GROUP BY CustID");

    Table3.show()

    spark.stop()

  }
}
