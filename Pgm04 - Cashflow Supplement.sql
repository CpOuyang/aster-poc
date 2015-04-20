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
where enc_acct_id
in
(
select enc_acct_id
from i.pos_card_session_statis 
where session_cnt=3
)a;
 

--利用NPATH直接找出一天內多次大額消費的紀錄
--drop table if exists i.card_statement_linkage;
--create dimension table i.card_statement_linkage as  --DIMENSION!!! calculation might be INCORRECT if applying distributed fields as source under multi-structure
--select
--    enc_acct_id, enc_cust_id, credit_limit
--    rank() over (partition by enc_acct_id order by credit_limit desc) as rank
--from (
--    select enc_acct_id, enc_cust_id, credit_limit from card_1410 union
--    select enc_acct_id, enc_cust_id, credit_limit from card_1411 union
--    select enc_acct_id, enc_cust_id, credit_limit from card_1412
--) as a
--;
--delete from i.card_statement_linkage where 1 < rank;


drop table if exists i.pos_npath_24hr_rslt;
create table i.pos_npath_24hr_rslt distribute by hash(Enc_Acct_Id) as
select enc_acct_id, /*enc_cust_id,*/register_name,mcht_id,accum_transaction_desc_chinese,Business_Type,business_name,begin_tran_tm,end_tran_tm,b_cnt+1 as txn_cnt,
a_amt+b_amt as txn_amt,accum_txn_amt--,perm_credit_line_amt
FROM NPATH(
ON (
select a.*,
b.register_name,
b.Business_Type,
b.business_name
--c.credit_limit as perm_credit_line_amt,
--c.enc_cust_id
--d. employment_years ,
--d. perm_credit_line_date,
--d. perm_credit_line_amt,
--d. temp_credit_line_amt,
--d. education_code,
--d. occupation_code,
--d. zip_code,
--d. company_name
from card_statement_01 a 
left join merchant_application b on trim(a.mcht_id) = trim(b.mcht_id)
--left join card_1412 c
--left join (
--    select distinct * from i.card_statement_linkage
--) c on trim(a.enc_acct_id)=trim(c.enc_acct_id)
--left join card_customer_1412 d on trim(c.enc_cust_id)=trim(d.Enc_Cust_ID)
)
PARTITION BY enc_acct_id
order by transaction_date 
PATTERN('A.B+')
MODE(NONOVERLAPPING)
SYMBOLS
(
TRUE AS A , 
--transaction_date -lag(transaction_date,1)<='240000'::time AS B 
--extract('date' from transaction_date) = extract('date' from lag(transaction_date,1)) as B
transaction_date = lag(transaction_date, 1) and mcht_id = lag(mcht_id, 1) as B
)
result
(
--first(enc_cust_id of A) AS Enc_Cust_Id,
first(transaction_date of A) AS BEGIN_tran_tm,
last(transaction_date of B) as end_tran_tm,
count(transaction_amt  OF B) as b_cnt,
first(Enc_Acct_Id  of A) as Enc_Acct_Id,
first(transaction_amt  of A) as a_amt,
SUM(transaction_amt  OF B) as b_amt,
first(mcht_id  of A) as mcht_id,
first(register_name of A) as register_name,
first(Business_Type of A) as Business_Type,
first(business_name of A) as business_name,
accumulate(transaction_amt   OF any (A,B)) as accum_txn_amt,
accumulate(transaction_desc_chinese   OF any (A,B)) as accum_transaction_desc_chinese
----first(credit_limit  of A) as credit_limit
--first(perm_credit_line_amt  of A) as perm_credit_line_amt
)
)
--where a_amt+b_amt>=credit_limit*0.8
--where a_amt+b_amt>=perm_credit_line_amt*0.7
;        


--針對以上的卡找出與其他商戶的消費狀況 以及是怎樣類型的人

