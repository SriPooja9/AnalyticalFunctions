import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem6 {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val Data=List(
      ("1", "karthik", "1000"),
      ("2", "mohan", "2000"),
      ("3", "vinay", "1500"),
      ("4", "Deva", "3000")
    ).toDF("id", "name", "salary")

    val wind=Window.orderBy($"id")

    val df=Data.withColumn("lead",lead($"salary",1).over(wind))

    df.withColumn("New",min($"lead").over(wind)).show()



  }

}
