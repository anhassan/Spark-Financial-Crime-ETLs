package transform

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import schema.{CreditTxn, Ecif}

object CreditTxnFraud {
  def transform(creditTxn: Dataset[CreditTxn], ecif: Dataset[Ecif], processingDate: String): DataFrame = {
    import creditTxn.sparkSession.implicits._

    ecif.drop("processing_date")
      .where('orgunit_num.isNotNull and 'prod_type_code === "INVT")
      .join(creditTxn, 'univ_acct_num === 'acct_num, "right")
      .withColumn("bank_id", coalesce('fi_code, lit("0000")))
      .withColumn("aml_party_id", coalesce('party_num,
        concat_ws("", 'prod_type_code, 'univ_acct_num)))
      .withColumn("txn_datetime", concat_ws(" ",
        'txn_date, 'txn_time.substr(1, 8)))
      .withColumn("txn_datetime_sub_second", 'txn_time.substr(7, 2).cast("int"))
      .withColumn("msg_type_code", lit("200"))
      .withColumn("src_appl_code", lit("CRD"))
      .withColumn("acct_type_code", 'fi_code.substr(4, 1))
      .withColumn("total_amount", 'txn_total_amt.cast("int") / 100)
      .withColumn("total_cash", 'txn_cash_amount.cast("int") / 100)
      .withColumn("txn_total_amt", 'total_amount.cast("int"))
      .withColumn("txn_cash_amt", 'total_cash.cast("int"))
      .withColumn("orig_boa_transit_num", when('orig_boa_transit_num === "0000", "UNK")
        .otherwise('orig_boa_transit_num))
      .withColumn("acct1_ledger_type_code", lit("ACC"))
      .withColumn("acct1_boa_transit_num", coalesce('orig_boa_transit_num, lit("UNK")))
      .withColumn("cust1_org_oper_name", when('party_typ_code === "O", 'trade_1_name))
      .withColumn("cust1_phone_num", concat_ws("", 'home_main_area_code,
        'home_main_phone_num, when('home_main_ext_num.isNotNull, 'home_main_ext_num)))
      .withColumnRenamed("txn_ref_num", "txn_reference_num")
      .withColumnRenamed("currency_code", "orig_currency_code")
      .withColumnRenamed("prsnl_last_name", "cust1_pers_last_name")
      .withColumnRenamed("prsnl_frst_name", "cust1_pers_first_name")
      .withColumnRenamed("legal_name_1", "cust1_org_legal_name")
      .withColumnRenamed("birth_date", "cust1_pers_birth_date")
      .withColumnRenamed("home_main_street_name", "cust1_addr_street")
      .withColumnRenamed("home_main_city", "cust1_addr_city_town")
      .withColumnRenamed("home_main_province_code", "cust1_addr_prov_state_name")
      .withColumnRenamed("home_main_postal_code", "cust1_addr_postal_zip_code")
      .withColumnRenamed("home_main_country_short_name", "cust1_addr_country_code")
  }
}
