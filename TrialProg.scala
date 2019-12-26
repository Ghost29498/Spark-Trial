import org.apache.spark.sql.SparkSession

object TrialProg extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()
  val name = "F:\\Crayon\\examples-master\\examples-master\\Search\\ReadTrial\\emp_data.csv"
  val  df = spark.read.format("csv").option("header", "true").load(name)
  println(df.show(10,false))


}
