import org.apache.spark.sql.SparkSession

object TrialProg extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()
  val name = "C:\\Users\\ShriramSubramanian\\Documents\\spark-trial\\ReadTrial\\top50.csv"
  val  df = spark.read.format("csv").option("header", "true").load(name)
  println(df.show(10,false))
  df.write.parquet("C:\\Users\\ShriramSubramanian\\Documents\\spark-trial\\ReadTrial\\top50.parquet")

}
