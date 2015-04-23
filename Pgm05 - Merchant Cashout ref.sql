--以card_bucket>0的角度看這些人的交易行為

drop table if exists i.card_statement_risk;
create table i.card_statement_risk distribute by hash(enc_acct_id) as
select a.* from card_statement a
left join i.card_statement_linkage b 
on trim(a.enc_acct_id) = trim(b.enc_acct_id)
where trim(b.enc_cust_id) in 
(
    select trim(enc_cust_id) from risk_1410 where card_bucket>0 union all
    select trim(enc_cust_id) from risk_1411 where card_bucket>0 union all
    select trim(enc_cust_id) from risk_1412 where card_bucket>0 
);


--在信用卡刷卡交易紀錄中,共有18718張卡是曾經有卡片延遲繳款的狀況
 count( distinct  trim(a.enc_acct_id))
---------------------------------------
                                 18718
(1 row)

--這些卡片共有38097筆交易紀錄
beehive=> select count(*) from i.card_statement_risk;
 count(1)
----------
    38097
(1 row)

--sessionize
drop table if exists i.pos_card_session_risk;
create table i.pos_card_session_risk distribute by hash(enc_acct_id) as
SELECT *
FROM SESSIONIZE (
    ON i.card_statement_risk
    PARTITION BY Enc_Acct_Id,Mcht_Id
    ORDER BY transaction_date
    TIMECOLUMN ('transaction_date')
    TIMEOUT ('2592000') --30 days
    RAPIDFIRE ('86399') 
);



--sessionize結果統計

drop table if exists i.pos_card_session_statis_risk;
create table i.pos_card_session_statis_risk distribute by hash(Enc_Acct_Id) as
select A1.Enc_Acct_Id,A1.Mcht_Id,
       A1.session_cnt+1 as session_cnt , 
       A1.rapidfire_cnt_24h
from 
  (
  select Enc_Acct_Id,Mcht_Id,
    max(sessionid) as session_cnt,
    sum ( case when rapidfire = 'true' then 1 else 0 end  ) as rapidfire_cnt_24h
  from i.pos_card_session_risk
  group by Enc_Acct_Id ,Mcht_Id
  ) A1;
  
 
--  有4%(90/18715=4%)的卡在這三個月每個月都有對同一個商戶有消費行為
beehive=> select count(distinct enc_acct_id) from i.pos_card_session_statis_risk where  session_cnt=3;
 count( distinct enc_acct_id)
------------------------------
                           90
(1 row)

-- 有4%(1451/18715=7%)的卡在這三個月有連續2個月都有對同一個商戶有消費行為
beehive=> select count(distinct enc_acct_id) from i.pos_card_session_statis_risk where  session_cnt=2;
 count( distinct enc_acct_id)
------------------------------
                         1451
(1 row)

-- 有11張卡符合每個月都有對同一個商戶消費 且曾有過在一天內對同個商戶刷卡大於0次
beehive=> select count(distinct enc_acct_id) from i.pos_card_session_statis_risk where  session_cnt=3 and rapidfire_cnt_24h>0;
 count( distinct enc_acct_id)
------------------------------
                           11
(1 row)

  
--npath  



drop table if exists i.pos_npath_24hr_rslt_risk;
create table i.pos_npath_24hr_rslt_risk distribute by hash(Enc_Acct_Id) as
select *
FROM NPATH (
    ON (
        select a.*,
        b.register_name,
        b.Business_Type,
        b.business_name,
        c.credit_limit,
        c.enc_cust_id,
        d.employment_years,
        d.perm_credit_line_date,
        d.perm_credit_line_amt,
        d.temp_credit_line_amt,
        d.education_code,
        d.occupation_code,
        d.zip_code,
        d.company_name
        --e.card_bucket,
        --e.loan_bucket,
       -- e.cust_bucket
        from i.card_statement_risk as a
        left join merchant_application b on trim(a.mcht_id) = trim(b.mcht_id)
        left join i.card_statement_linkage c on trim(a.enc_acct_id) = trim(c.enc_acct_id)
        left join card_customer_1412 d on trim(c.enc_cust_id) = trim(d.enc_cust_id)
       -- left join risk_1412 e on trim(c.enc_cust_id) = trim(d.enc_cust_id)
    )
    PARTITION BY enc_acct_id, mcht_id
    order by transaction_date 
    -- PATTERN('A.B+')
    PATTERN('A*')
    --PATTERN('A.B{9999,}|A.X.C0+.C1{1,2}.C0*|A.X.C0+.C2.C0*|A.X.C1.C0*.C1.C0*|A.X.C1{1,2}.C0*|A.X.C2.C0*|A.X.C2.C0*.C1.C0*|A.X.C2.C1.C0*')
    MODE(NONOVERLAPPING)
    SYMBOLS(
        TRUE AS A
        /*transaction_date = lag(transaction_date, 1) and trim(mcht_id) = trim(lag(mcht_id, 1)) as B,
        (trim(mcht_id) = trim(lag(mcht_id, 1))) or (transaction_date::date - lag(transaction_date, 1)::date >= 3) as X,
        transaction_date::date - lag(transaction_date, 1)::date = 0 as C0,
        transaction_date::date - lag(transaction_date, 1)::date = 1 as C1,
        transaction_date::date - lag(transaction_date, 1)::date = 2 as C2,
        transaction_date::date - lag(transaction_date, 1)::date >= 3 as C3*/
    )
    result(
        --FIRST(enc_cust_id of A) AS Enc_Cust_Id,
        FIRST(Enc_Acct_Id  of A) as Enc_Acct_Id,
        --FIRST(mcht_id  of A) as mcht_id,
        --FIRST(transaction_date of A) AS BEGIN_tran_tm,
        --LAST(transaction_date of B) as end_tran_tm,
        count(transaction_amt  OF A) as txn_cnt,
        SUM(transaction_amt of ANY(A)) as txn_amt,
        ACCUMULATE(mcht_id of A) as mcht_id,
        ACCUMULATE(register_name of A) as register_name,
        ACCUMULATE(Business_Type of A) as Business_Type,
        ACCUMULATE(business_name of A) as business_name,
        ACCUMULATE(transaction_amt OF A) as accum_txn_amt,
        ACCUMULATE(transaction_date OF A) as accum_txn_dt,
        ACCUMULATE(trim(transaction_desc_chinese) OF A) as accum_transaction_desc_chinese,
        FIRST(employment_years of A) as Employment_Years,
        FIRST(perm_credit_line_date of A) as Perm_Credit_Line_Date,
        FIRST(perm_credit_line_amt of A) as Perm_Credit_Line_Amt,
        FIRST(temp_credit_line_amt of A) as Temp_Credit_Line_Amt,
        FIRST(education_code of A) as Education_Code,
        FIRST(occupation_code of A) as Occupation_Code,
        FIRST(zip_code of A) as Zip_Code,
        FIRST(company_name of A) as Company_Name
       -- FIRST(card_bucket of A) as Card_Bucket,
       -- FIRST(loan_bucket of A) as Loan_Bucket,
       -- FIRST(cust_bucket of A) as Cust_Bucket
    )
)
where perm_credit_line_amt * 0.6 <= txn_amt
order by 1, 2, 3
;

--GRAPHGEN

select *
from GraphGen (
    on (select business_type,count(*) as cnt from i.pos_npath_24hr_rslt_risk group by 1 having count(*)>1 )
    partition BY 1
    order by cnt desc
    item_format('npath')
    SCORE_COL('cnt')
    ITEM1_COL('business_type')
    OUTPUT_FORMAT('sankey')
    nodesize_max(5) nodesize_min(1)
    domain('192.168.31.134')
    title('npath')
)
;
