package etl

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import schema.{CreditTxn, Ecif, FraudTxn}
import transform.CreditTxnFraud
import utils.EtlUtils._


object CreditTxnEtl extends App {

  val spark = SparkSession.builder()
    .appName("Spark Generic Etl")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // getting the data source & sink credentials
  val config = ConfigFactory.load("configs/application.conf")

  // fetching source credentials
  val creditFeed = config.getString("feeds.source.creditfeed")
  val ecifFeed = config.getString("feeds.source.eciffeed")
  val ecifDatabase = config.getString("feeds.source.ecifdatabase")

  // fetching sink credentials
  val fraudFeed = config.getString("feeds.sink.fraudfeed")
  val fraudDatabase = config.getString("feeds.sink.frauddatabase")

  // getting the ETL processing date
  val processingDate = java.time.LocalDate.now.toString

  // extracting data from multiple feed sources
  val creditTxn = loadFlatFile(creditFeed).as[CreditTxn]
  val ecifTxn = loadTable(ecifFeed, ecifDatabase).as[Ecif]

  // transforming input feed data
  val fraudTxn = acquire[FraudTxn](CreditTxnFraud.transform(creditTxn,
    ecifTxn, processingDate))

  // loading the transformed data into sink
  insertInto[FraudTxn](fraudTxn, fraudFeed, fraudDatabase)

}


