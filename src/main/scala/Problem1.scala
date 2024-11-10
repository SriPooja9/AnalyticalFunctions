import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag

object Problem1 {

def main(args:Array[String]):Unit= {

val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

  import spark.implicits._

  val ChocData=List(
    ("1","KitKat","1000.0","2021-01-01"),
    ("1","KitKat","2000.0","2021-01-02"),
    ("1","KitKat","1000.0","2021-01-03"),
    ("1","KitKat","2000.0","2021-01-04"),
    ("1","KitKat","3000.0","2021-01-05"),
    ("1","KitKat","1000.0","2021-01-06")
  )
    .toDF("it_id","it_name","price","pricedate")

  val wind=Window.orderBy($"pricedate")

  ChocData.
    withColumn("prev_price",lag($"price",1).over(wind)).
    withColumn("diff",$"price"-lag($"price",1).over(wind)).show()


}

}
