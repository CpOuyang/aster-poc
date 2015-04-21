--0416 信用卡套現分析

--利用sessionize識別刷卡週期性與短時間內大額消費
 
drop table if exists i.pos_card_session;
create table i.pos_card_session distribute by hash(enc_acct_id) as
SELECT *
FROM SESSIONIZE (
    ON card_statement
    PARTITION BY Enc_Acct_Id,Mcht_Id
    ORDER BY transaction_date
    TIMECOLUMN ('transaction_date')
    TIMEOUT ('2592000') --30天算1個session
    RAPIDFIRE ('86399') --1天有多筆就代表有问题
--    RAPIDFIRE ('86401') --1天有多筆就代表有问题
);

--sessionize結果統計

drop table if exists i.pos_card_session_statis;
create table i.pos_card_session_statis distribute by hash(Enc_Acct_Id) as
select A1.Enc_Acct_Id,A1.Mcht_Id,
       A1.session_cnt+1 as session_cnt , 
       A1.rapidfire_cnt_24h
from 
  (
  select Enc_Acct_Id,Mcht_Id,
    max(sessionid) as session_cnt,
    sum ( case when rapidfire = 'true' then 1 else 0 end  ) as rapidfire_cnt_24h
  from i.pos_card_session
  group by Enc_Acct_Id ,Mcht_Id
  ) A1;
  

drop table if exists card_statement_01;
create table  card_statement_01 distribute by hash(enc_acct_id) as
select * from card_statement
where enc_acct_id in (
    select enc_acct_id
    from i.pos_card_session_statis 
    where 2 <= session_cnt
)
;
 

--利用NPATH直接找出一天內多次大額消費的紀錄
drop table if exists i.card_statement_linkage;
create table i.card_statement_linkage distribute by hash(enc_acct_id) as
select
    enc_acct_id, enc_cust_id, credit_limit,
    rank() over (partition by enc_acct_id order by as_of_date desc) as rank
from (
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141001'::date as as_of_date from card_1410 union all
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141101'::date as as_of_date from card_1411 union all
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141201'::date as as_of_date from card_1412
) as a
;
delete from i.card_statement_linkage where 1 < rank;
alter table i.card_statement_linkage drop column rank;


drop table if exists i.pos_npath_24hr_rslt;
create table i.pos_npath_24hr_rslt distribute by hash(Enc_Acct_Id) as
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
        from card_statement_01 as a
        left join merchant_application b on trim(a.mcht_id) = trim(b.mcht_id)
        left join i.card_statement_linkage c on trim(a.enc_acct_id) = trim(c.enc_acct_id)
        left join card_customer_1412 d on trim(c.enc_cust_id) = trim(d.enc_cust_id)
       -- left join risk_1412 e on trim(c.enc_cust_id) = trim(d.enc_cust_id)
    )
    PARTITION BY enc_acct_id, mcht_id
    order by transaction_date 
    -- PATTERN('A.B+')
    PATTERN('A.B{9999,}|A.X.C0+.C1{1,2}.C0*|A.X.C0+.C2.C0*|A.X.C1.C0*.C1.C0*|A.X.C1{1,2}.C0*|A.X.C2.C0*|A.X.C2.C0*.C1.C0*|A.X.C2.C1.C0*')
    MODE(NONOVERLAPPING)
    SYMBOLS(
        TRUE AS A,
        transaction_date = lag(transaction_date, 1) and trim(mcht_id) = trim(lag(mcht_id, 1)) as B,
        (trim(mcht_id) = trim(lag(mcht_id, 1))) or (transaction_date::date - lag(transaction_date, 1)::date >= 3) as X,
        transaction_date::date - lag(transaction_date, 1)::date = 0 as C0,
        transaction_date::date - lag(transaction_date, 1)::date = 1 as C1,
        transaction_date::date - lag(transaction_date, 1)::date = 2 as C2,
        transaction_date::date - lag(transaction_date, 1)::date >= 3 as C3
    )
    result(
        FIRST(enc_cust_id of A) AS Enc_Cust_Id,
        FIRST(Enc_Acct_Id  of A) as Enc_Acct_Id,
        FIRST(mcht_id  of A) as mcht_id,
        FIRST(transaction_date of A) AS BEGIN_tran_tm,
        LAST(transaction_date of B) as end_tran_tm,
        count(transaction_amt  OF ANY(A, B)) as txn_cnt,
        SUM(transaction_amt of ANY(A, B)) as txn_amt,
        FIRST(register_name of A) as register_name,
        FIRST(Business_Type of A) as Business_Type,
        FIRST(business_name of A) as business_name,
        ACCUMULATE(transaction_amt OF any (A, B)) as accum_txn_amt,
        ACCUMULATE(trim(transaction_desc_chinese) OF any (A, B)) as accum_transaction_desc_chinese,
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

select * from i.pos_npath_24hr_rslt
where trim(Enc_Cust_Id) in (
	select trim(Enc_Cust_Id)
	from (
		select trim(enc_cust_id) as enc_cust_id from risk_1410 where 0 < card_bucket union
		select trim(enc_cust_id) as enc_cust_id from risk_1411 where 0 < card_bucket union
		select trim(enc_cust_id) as enc_cust_id from risk_1412 where 0 < card_bucket
	) as a
)
;

--針對以上的卡找出與其他商戶的消費狀況 以及是怎樣類型的人
