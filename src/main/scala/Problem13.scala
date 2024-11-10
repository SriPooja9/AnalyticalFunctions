import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem13
{

  def main(args:Array[String]):Unit={

    val conf=new SparkConf()
    conf.set("spark.appname","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val Data=List(
      ("1", "karthik", "1000"),
      ("2", "mohan", "2000"),
      ("3", "vinay", "1500"),
      ("4", "Deva", "3000")
    ).toDF("id", "name", "salary")

    val wind=Window.partitionBy($"name").orderBy($"id")

    Data.withColumn("Avg_Sal",avg($"salary").over(wind))
    .withColumn("Diff_Salary",$"salary"-$"Avg_Sal")
    .show()

  }

}
