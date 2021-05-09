package transform

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.matchers.must.Matchers.{contain, theSameElementsAs}
import schema.{CreditTxn, Ecif, FraudTxn}
import utils.EtlUtils._


class CreditTxnTest extends AnyFunSuite {

  val sparkSession = SparkSession.builder()
    .appName("Spark ETL Generic Test")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  val processingDate = "2021-05-08"

  // Getting the default data
  val ecifData = Map[String, String](
    "prod_type_code" -> "INVT",
    "acct_num" -> "ACCT_NUM",
    "aml_party_id" -> "AML_ID",
    "party_typ_code" -> "O",
    "trade_1_name" -> "TRADE_1",
    "home_main_area_code" -> "AREA_CODE",
    "home_main_phone_num" -> "PHONE_NUM",
    "home_main_ext_num" -> "EXT_NUM",
    "home_main_street_name" -> "STREET_NAME",
    "home_main_province_code" -> "PROVINCE_CODE",
    "home_main_postal_code" -> "POSTAL_CODE",
    "home_main_country_short_name" -> "CNTRY_SHORT",
    "prsnl_last_name" -> "LAST_NAME",
    "prsnl_frst_name" -> "FIRST_NAME",
    "legal_name_1" -> "LEGAL_1",
    "birth_date" -> "BIRTH_DATE",
    "party_num" -> "123"
  )

  val creditTxnData = Map[String, String](
    "univ_acct_num" -> "ACCT_NUM",
    "fi_code" -> "BANKX_ID",
    "txn_date" -> "2021-05-02",
    "txn_time" -> "12:13:14",
    "txn_total_amt" -> "15000",
    "txn_cash_amount" -> "23000",
    "orig_boa_transit_num" -> "ORIG_BOA_TRANSIT",
    "txn_ref_num" -> "TXN_REF",
    "currency_code" -> "CURR_CODE",
    "txn_code" -> "TXN_CODE",
    "txn_cat_code" -> "TXN_CAT_CODE",
    "cr_dr_ind" -> "CR_DR_IND"
  )

  // Creating datasets out of the default data
  val creditTxn = acquire[CreditTxn](dataToDF(creditTxnData))
  val ecifTxn = acquire[Ecif](dataToDF(ecifData)
    .withColumn("party_num", 'party_num.cast("int")))

  // Creating empty datasets for txns
  val creditTxnEmpty = sparkSession.emptyDataset[CreditTxn]
  val ecifTxnEmpty = sparkSession.emptyDataset[Ecif]

  test("join_condition") {

    val creditTxnUpdated = acquire[CreditTxn](dataToDF(creditTxnData).union(dataToDF(creditTxnData)
      .withColumn("univ_acct_num", lit("UNIV_ACCT_NUM"))
      .withColumn("txn_ref_num", lit("TXN_REF_0"))))

    val result = CreditTxnFraud.transform(creditTxnUpdated, ecifTxn, processingDate)
    result.count() should be(2)
    result.select("acct_num", "txn_reference_num").collect() should contain theSameElementsAs (Seq(
      Row("ACCT_NUM", "TXN_REF"), Row(null, "TXN_REF_0")
    ))
  }

  test("remaining_fields") {

    val result = CreditTxnFraud.transform(creditTxn, ecifTxn, processingDate)
    val resultDS = acquire[FraudTxn](result).head

    resultDS.bank_id should be("BANKX_ID")
    resultDS.aml_party_id should be("123")
    resultDS.txn_datetime should be("2021-05-02 12:13:14")
    resultDS.txn_total_amt should be(150)
    resultDS.txn_cash_amt should be(230)
    resultDS.txn_datetime_sub_second should be(14)
    resultDS.msg_type_code should be("200")
    resultDS.src_appl_code should be("CRD")
    resultDS.acct_type_code should be("K")
    resultDS.txn_reference_num should be("TXN_REF")
    resultDS.txn_cat_code should be("TXN_CAT_CODE")
    resultDS.txn_code should be("TXN_CODE")
    resultDS.orig_boa_transit_num should be("ORIG_BOA_TRANSIT")
    resultDS.acct1_ledger_type_code should be("ACC")
    resultDS.acct1_boa_transit_num should be("ORIG_BOA_TRANSIT")
    resultDS.cust1_phone_num should be("AREA_CODEPHONE_NUMEXT_NUM")
    resultDS.cust1_pers_first_name should be("FIRST_NAME")
    resultDS.cust1_pers_last_name should be("LAST_NAME")
    resultDS.cust1_org_legal_name should be("LEGAL_1")
    resultDS.cust1_pers_birth_date should be("BIRTH_DATE")
    resultDS.cust1_addr_country_code should be("CNTRY_SHORT")
    resultDS.cust1_addr_postal_zip_code should be("POSTAL_CODE")
    resultDS.cust1_addr_prov_state_name should be("PROVINCE_CODE")

  }

}
