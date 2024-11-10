import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, when}

object Problem2
{
  def main(args:Array[String]):Unit={

    val conf=new SparkConf()
    conf.set("spark.appname","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val emp=List(("1","John","1000","01/01/2016"),
      ("1","John","2000","02/01/2016"),
      ("1","John","1000","03/01/2016"),
      ("1","John","2000","04/01/2016"),
      ("1","John","3000","05/01/2016"),
      ("1","John","1000","06/01/2016")).toDF("id","name","salary","join_date")

    val wind=Window.orderBy($"join_date")

    val df3=emp.withColumn("lag",lag($"salary",1).over(wind)).select("*")

    df3.withColumn("classify",when ($"salary" < $"lag", "DOWN")
      .when($"salary" > $"lag", "UP").otherwise("")).show()


  }
}
