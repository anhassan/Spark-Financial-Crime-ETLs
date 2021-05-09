package schema

case class CreditTxn(
                    branch_id_of_acct: String,
                    univ_acct_num : String,
                    fi_code : String,
                    channel_type_code: String,
                    txn_date : String,
                    txn_time : String,
                    txn_posting_date : String,
                    txn_ref_num : String,
                    txn_code : String,
                    txn_cat_code : String,
                    currency_code : String,
                    txn_total_amt:String,
                    txn_cash_amount : String,
                    cr_dr_ind : String,
                    cash_code : String,
                    orig_boa_transit_num : String,
                    processing_date : String
                    )
