import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Solution {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder().appName("HacktronT20").master("local[*]").getOrCreate()

    val productSchema = StructType(Array
    (StructField("product_id", IntegerType, true),
      StructField("product_name", StringType, true),
      StructField("product_type", StringType, true),
      StructField("product_version", StringType, true),
      StructField("product_price", StringType, true)
    )
    )

    val productDf = spark.read.option("delimiter", "|").schema(productSchema)
      .csv("C:\\Users\\anand_jha2\\AnandJha\\POC\\DBSHacktronT20\\data\\input\\Product.txt")


    productDf.show(5, false)

  }

}
