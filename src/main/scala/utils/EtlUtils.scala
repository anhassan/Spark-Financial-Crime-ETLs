package utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import scala.reflect.runtime.universe._

object EtlUtils {
  val sparkSession = SparkSession.builder()
    .appName("Spark Generic ETL")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  def dataToDF(data: Map[String, String]): DataFrame = {
    import sparkSession.implicits._

    val fields = data.map(row => row._1).toList
    val values = data.map(row => row._2).toList

    val df = Seq((values)).toDF("value")
    val columnsDF = (fields.indices).map(index =>
      col("value")(index).as(fields(index)))
    df.select(columnsDF: _*)
  }

  def acquire[T: Encoder : TypeTag](df: DataFrame): Dataset[T] = {

    val schemaSrc = df.schema.toList
    val schemaTgt = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType].toList

    val commonFields = schemaTgt.flatMap(fieldTgt =>
      schemaSrc.map(fieldSrc => (fieldSrc.name, fieldSrc.dataType) match {
        case (fieldTgt.name, fieldTgt.dataType) => fieldTgt.name
        case _ => null
      }))
      .filterNot(field => field == null)

    val differentFields = schemaTgt.map(field => field.name).filterNot(
      commonFields.toSet)

    val commonColumns = commonFields.map(field => col(field))

    val filteredDF = df.select(commonColumns: _*)
    differentFields.foldLeft(filteredDF) { (filDf, field) =>
      filDf.withColumn(field, lit(null))
    }.as[T]
  }

}
