import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {
 val sparkSession = SparkSession.builder()
   .appName("Spark Generic ETL")
   .master("local[2]")
   .getOrCreate()
}
