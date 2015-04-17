act -U beehive -w beehive

drop table if exists deposit_transaction_1411;
create table deposit_transaction_1411 (
    Enc_Acct_Id       varchar(256),
    Enc_Trans_Acct_Id varchar(256),
    Actual_Txn_Date   date,
    Actual_Txn_Time   time,
    Bal_After_Txn     decimal(20,3),
    Branch_Nbr        varchar(256),
    Foreign_Txn_Flag  varchar(256),
    Journal_Nbr       int,
    Posting_Date      date,
    Promotion_Code    varchar(256),
    Remark            varchar(256),
    Seq_No            varchar(256),
    Txn_Amt           decimal(20,3),
    Txn_Channel_Code  varchar(256),
    Txn_Code          int,
    Txn_Date          date,
    Txn_Status_Code   varchar(256)
) distribute by hash(enc_acct_id)
;
select * from deposit_transaction_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive deposit_transaction_1411 deposit_transaction_1411.txt -c



drop table if exists deposit_1411;
create table deposit_1411 (
    Enc_Acct_Id               varchar(256),
    Enc_Cust_Id               varchar(256),
    Acct_Open_Date            date,
    Acct_Ownership_Cnt        int,
    Acct_Status_Code          varchar(256),
    Acct_Type_Code            varchar(256),
    Bank_Merged_Flag          varchar(256),
    Branch_Nbr                varchar(256),
    Business_Branch_Nbr       varchar(256),
    Business_Line_Code        varchar(256),
    Business_Type_Code        varchar(256),
    Currency_Code             varchar(256),
    Current_Bal               decimal(20,3),
    Current_Int_Rate          decimal(20,5),
    Dep_Acct_Code             varchar(256),
    Donate_Flag               varchar(256),
    Financing_Code            varchar(256),
    Financing_Hold_Amt        decimal(20,3),
    Financing_Type            varchar(256),
    Full_Cash_Delivery_Flag   varchar(256),
    Fund_Flag                 varchar(256),
    Future_Type_Code          varchar(256),
    Highest_OD_Int_Flag       varchar(256),
    Int_Category_Code         varchar(256),
    Inter_Bank_Withdraw_Cnt   int,
    Inter_Branch_Auth_Code    varchar(256),
    Int_From_Date             date,
    Int_To_Date               date,
    Last_Fin_Txn_Date         date,
    Last_Hold_Date            date,
    Last_Int_Withdraw_Date    date,
    Overdraft_Int_Accrued_Amt decimal(20,3),
    Overdraft_Int_Rate        decimal(20,5),
    Rate_Margin               decimal(20,5),
    Rate_Type_Code            varchar(256),
    Re_Consigned_Flag         varchar(256),
    Stand_Alone_Limit_Amt     bigint,
    Term_Basis_Code           varchar(256),
    Term_Maturity_Date        date,
    Term_Pay_Freq_Code        varchar(256),
    Term_Principal_Bal        decimal(20,3),
    Term_Start_Date           date,
    This_Term_Length          int
) distribute by hash(enc_acct_id)
;
select * from deposit_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive deposit_1411 deposit_1411.txt -c



drop table if exists card_statement_1411;
create table card_statement_1411 (
    Enc_Acct_Id              varchar(256),
    Mcht_Id                  varchar(256),
    Currency_Code            varchar(256),
    Currency_Transaction_Amt decimal(20,3),
    Mcc_Code                 varchar(256),
    Microfilm_Id             varchar(256),
    Pos_Entry_Mode           varchar(256),
    Product_Id               varchar(256),
    Statement_Date           date,
    Statement_Sequence_Nbr   int,
    Transaction_Amt          decimal(20,3),
    Transaction_City         varchar(256),
    Transaction_Code         varchar(256),
    Transaction_Country_Code varchar(256),
    Transaction_Date         date,
    Transaction_Desc_Chinese varchar(256),
    Transaction_Posting_Date date
) distribute by hash(enc_acct_id)
;
select * from card_statement_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive card_statement_1411 card_statement_1411.txt -D '\t'



drop table if exists card_1411;
create table card_1411 (
    Enc_Acct_Id                  varchar(256),
    Enc_Cust_Id                  varchar(256),
    Appl_Id                      varchar(256),
    Enc_Idv_Cust_Id              varchar(256),
    Card_Expiration_Year_Month   int,
    Card_Ownership_Code          varchar(256),
    Acct_Open_Date               date,
    Billing_Cycle_Code           varchar(256),
    Product_Id                   varchar(256),
    Block_Transaction_Date       date,
    Prev_Block_Transaction_Date  date,
    Cash_Balance                 bigint,
    Chargeoff_Date               date,
    Cash_Chargeoff_Amt           decimal(20,3),
    Collection_Start_Date        date,
    Last_Credit_Limit            bigint,
    Retail_Balance               bigint,
    Retail_Chargeoff_Amt         decimal(20,5),
    Retail_Collection_Interest   decimal(20,5),
    Credit_Limit                 bigint,
    Perm_Credit_Limit_Date       date,
    Perm_Credit_Limit_Amt        bigint,
    Temp_Credit_Limit_Start_Date date,
    Temp_Credit_Limit_End_Date   date,
    Temp_Credit_Limit_Amt        bigint,
    Last_Delinquent_Date         date,
    Fixed_Payment_Amt            bigint,
    Last_Payment_Date            date,
    Last_Payment_Amt             decimal(20,3),
    Affinity_Group_Code          varchar(256),
    Block_Code                   varchar(256),
    Card_Circulation_Code        varchar(256),
    Chargeoff_Code               varchar(256),
    Collection_Block_Code        varchar(256),
    Prev_Block_Code              varchar(256),
    Cycle_Status_Code            varchar(256)
) distribute by hash(enc_acct_id)
;
select * from card_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive card_1411 card_1411.txt -c



drop table if exists card_customer_1411;
create table card_customer_1411 (
    Enc_Cust_Id           varchar(256),
    Enc_Dir_Acct_Id       varchar(256),
    Employment_Years      int,
    _Birth_Date           varchar,
    Perm_Credit_Line_Date date,
    Perm_Credit_Line_Amt  bigint,
    Temp_Credit_Line_Amt  bigint,
    Education_Code        varchar(256),
    Gender_Code           varchar(256),
    Marital_Status_Code   varchar(256),
    Occupation_Code       varchar(256),
    Zip_Code              varchar(256),
    Company_Name          varchar(256)
) distribute by hash(enc_cust_id)
;
select * from card_customer_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive card_customer_1411 card_customer_1411.txt -c -D '\t'
fg
alter table card_customer_1411 add column Birth_Date date;
update card_customer_1411 set birth_date = _birth_date::date;
alter table card_customer_1411 drop column _birth_date;


drop table if exists loan_customer_1411;
create table loan_customer_1411 (
    Enc_Cust_Id          varchar(256),
    Enc_Acct_Id          varchar(256),
    Bill_Cycle           varchar(256),
    Business_Line_Code   varchar(256),
    Company_Name         varchar(256),
    Contact_Zip_Code     varchar(256),
    Customer_Type_Code   varchar(256),
    Birth_Date           date,
    Dead_Flag            varchar(256),
    Education_Code       varchar(256),
    Gender_Code          varchar(256),
    Graduate_Year_Month  varchar(256),
    Home_Branch_Nbr      varchar(256),
    House_Ownership_Code varchar(256),
    Last_Bill_Date       date,
    Last_Update_Date     date,
    Loan_Contract_Flag   varchar(256),
    Marital_Status_Code  varchar(256),
    Nationality_Code     varchar(256),
    Personal_Income      bigint,
    Register_Zip_Code    varchar(256),
    Rel_Start_Date       date,
    Wm_Review_Date       date,
    Wm_Start_Date        date
) distribute by hash(enc_acct_id)
;
select * from loan_customer_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive loan_customer_1411 loan_customer_1411.txt -c




drop table if exists loan_1411;
create table loan_1411 (
    Enc_Acct_Id             varchar(256),
    Enc_Cust_Id             varchar(256),
    Acct_Status_Code        varchar(256),
    Acct_Type_Code          varchar(256),
    Application_Amt         decimal(20,2),
    Application_Date        date,
    Approval_Amt            bigint,
    Approval_Date           date,
    Approval_Expiry_Date    date,
    Arrear_Days             int,
    Arrear_Reason_Code      varchar(256),
    Bad_Debit_Bal           bigint,
    Bad_Debit_Code          varchar(256),
    Bad_Debit_Date          date,
    Bad_Debit_Reason_Code   varchar(256),
    Branch_Nbr              varchar(256),
    Business_Branch_Nbr     varchar(256),
    Business_Line_Code      varchar(256),
    Collection_Bal          decimal(20,3),
    Collection_Date         date,
    Currency_Code           varchar(256),
    Current_Bal             bigint,
    Current_Int_Rate        decimal(20,5),
    First_Loan_Funding_Date date,
    Funding_Amt             decimal(20,3),
    Grace_Expiry_Date       date,
    Guarantee_Rate          double,
    Int_Category_Code       varchar(256),
    Enc_Int_Tfr_Acct_Nbr    varchar(256),
    Last_Arrear_Date        date,
    Last_Fin_Txn_Date       date,
    Last_Non_Fin_Txn_Date   date,
    Loan_Funding_Date       date,
    Maturity_Date           date,
    Min_Pay_Amt             decimal(20,3),
    Penalty_Due_Date        date,
    Penalty_Ratio           double,
    Purpose_Code            varchar(256),
    Remaining_Repay_Term    int,
    Repayment_Amt           decimal(20,3),
    Term_Basis_Code         varchar(256),
    Theo_Current_Bal        decimal(20,3),
    This_Term_Length        int,
    Total_Loan_Term         int
) distribute by hash(enc_acct_id)
;
select * from loan_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive loan_1411 loan_1411.txt -c




drop table if exists merchant_profile_1411;
create table merchant_profile_1411 (
    Mcht_Id             varchar(256),
    Enc_Acct_Id         varchar(256),
    Bank_Code           varchar(256),
    Corp_Id             varchar(256),
    Chain_Mcht_Id       varchar(256),
    Master_Mcht_Id      varchar(256),
    Last_Rate_Adjust_Dt date,
    Open_Date           date,
    Merchant_Type_Code  varchar(256),
    Terminate_Date      date
) distribute by hash(mcht_id)
;
select * from merchant_profile_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive merchant_profile_1411 merchant_profile_1411.txt -c



drop table if exists merchant_application;
create table merchant_application (
    Corp_Id       varchar(256),
    Mcht_Id       varchar(256),
    Enc_Owner_Id  varchar(256),
    Enc_Mngr_Id   varchar(256),
    Appl_Id       varchar(256),
    Business_Name varchar(256),
    Business_Type varchar(256),
    Register_Name varchar(256),
    Branch        varchar(256),
    Bank          varchar(256),
    Build_Date    date
) distribute by hash(mcht_id)
;
select * from merchant_application limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive merchant_application merchant_application.txt -c



drop table if exists risk_1411;
create table risk_1411 (
    Enc_Cust_Id varchar(256),
    Card_Bucket int,
    Loan_Bucket int,
    Cust_Bucket int
) distribute by hash(Enc_Cust_Id)
;
select * from risk_1411 limit 20;

^z
ncluster_loader -h 192.168.31.134 -U beehive -w beehive risk_1411 risk_1411.txt -c
