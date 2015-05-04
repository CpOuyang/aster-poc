--0416 信用卡套現分析

drop table if exists card_statement;
create table card_statement distribute by hash(Enc_Acct_Id) as
select * from (
    select *, '20141001'::timestamp as Snapshot from card_statement_1410 union all
    select *, '20141101'::timestamp as Snapshot from card_statement_1411 union all
    select *, '20141201'::timestamp as Snapshot from card_statement_1412
) as a
where trim(Mcht_Id) <> '000000000'
    and  5000 < Transaction_Amt
    and trim(Transaction_code) = '40' --Retail Transaction
    and transaction_desc_chinese not like '%分期%'
;
select count(*) from card_statement;

alter table card_statement alter column transaction_date type timestamp;


--利用sessionize識別刷卡週期性與短時間內大額消費
drop table if exists card_statement_ss;
create table card_statement_ss distribute by hash(enc_acct_id) as
select *
from sessionize (
    on card_statement
    partition by enc_acct_id, mcht_id
    order by transaction_date
    timecolumn ('transaction_date')
    timeout ('2592000') --30天算1個session
    rapidfire ('86399') --1天有多筆就代表有问题
)
;

--sessionize結果統計
drop table if exists card_statement_ss_stat;
create table card_statement_ss_stat distribute by hash(Enc_Acct_Id) as
select a.Enc_Acct_Id,
    a.Mcht_Id,
    a.session_cnt + 1 as session_cnt,
    a.rapidfire_cnt
from (
    select Enc_Acct_Id, Mcht_Id,
        max(sessionid) as session_cnt,
        sum(case when rapidfire = 'true' then 1 else 0 end) as rapidfire_cnt
    from card_statement_ss
    group by Enc_Acct_Id, Mcht_Id
) as a
;



--利用NPATH直接找出一天內多次大額消費的紀錄
drop table if exists card_statement_link;
create table card_statement_link distribute by hash(enc_acct_id) as
select
    enc_acct_id, enc_cust_id, credit_limit,
    rank() over (partition by enc_acct_id order by as_of_date desc) as rank
from (
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141001'::date as as_of_date from card_1410 union all
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141101'::date as as_of_date from card_1411 union all
    select trim(enc_acct_id) as enc_acct_id, trim(enc_cust_id) as enc_cust_id, credit_limit, '20141201'::date as as_of_date from card_1412
) as a
;
delete from card_statement_link where 1 < rank;
alter table card_statement_link drop column rank;


drop table if exists card_statement_np;
create table card_statement_np distribute by hash(enc_acct_id) as
select *
from npath (
    on (select a.*,
            b.register_name,
            b.business_type,
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
            d.company_name,
            e.card_bucket,
            e.loan_bucket,
            e.cust_bucket
        from (select * from card_statement
              where trim(enc_acct_id) in (
                select trim(enc_acct_id)
                from card_statement_ss_stat
                where 2 <= session_cnt)) as a
        left join merchant_application as b on trim(a.mcht_id) = trim(b.mcht_id)
        left join card_statement_link as c on trim(a.enc_acct_id) = trim(c.enc_acct_id)
        left join card_customer_1412 as d on trim(c.enc_cust_id) = trim(d.enc_cust_id)
        left join risk_1412 as e on trim(c.enc_cust_id) = trim(e.enc_cust_id)
    )
    partition by enc_acct_id, mcht_id
    order by transaction_date 
    pattern('A.B+')
    -- pattern('A.B{9999,}|A.X.C0+.C1{1,2}.C0*|A.X.C0+.C2.C0*|A.X.C1.C0*.C1.C0*|A.X.C1{1,2}.C0*|A.X.C2.C0*|A.X.C2.C0*.C1.C0*|A.X.C2.C1.C0*')
    mode(nonoverlapping)
    symbols(
        true as A,
        transaction_date = lag(transaction_date, 1) and trim(mcht_id) = trim(lag(mcht_id, 1)) as B--,
        -- (trim(mcht_id) = trim(lag(mcht_id, 1))) or (transaction_date::date - lag(transaction_date, 1)::date >= 3) as X,
        -- 0 = transaction_date::date - lag(transaction_date, 1)::date as C0,
        -- 1 = transaction_date::date - lag(transaction_date, 1)::date as C1,
        -- 2 = transaction_date::date - lag(transaction_date, 1)::date as C2,
        -- 3 <= transaction_date::date - lag(transaction_date, 1)::date as C3
    )
    result(
        first(enc_cust_id of a) as enc_cust_id,
        first(enc_acct_id  of a) as enc_acct_id,
        first(mcht_id  of a) as mcht_id,
        first(transaction_date of a) as begin_tran_tm,
        last(transaction_date of b) as end_tran_tm,
        count(transaction_amt  of any(a, b)) as txn_cnt,
        sum(transaction_amt of any(a, b)) as txn_amt,
        first(register_name of a) as register_name,
        first(business_type of a) as business_type,
        first(business_name of a) as business_name,
        accumulate(transaction_amt of any (a, b)) as accum_txn_amt,
        accumulate(trim(transaction_desc_chinese) of any (a, b)) as accum_transaction_desc_chinese,
        first(employment_years of a) as employment_years,
        first(perm_credit_line_date of a) as perm_credit_line_date,
        first(perm_credit_line_amt of a) as perm_credit_line_amt,
        first(temp_credit_line_amt of a) as temp_credit_line_amt,
        first(education_code of a) as education_code,
        first(occupation_code of a) as occupation_code,
        first(zip_code of a) as zip_code,
        first(company_name of a) as company_name,
        first(card_bucket of a) as card_bucket,
        first(loan_bucket of a) as loan_bucket,
        first(cust_bucket of a) as cust_bucket
    )
)
where perm_credit_line_amt * 0.6 <= txn_amt
order by 1, 2, 3
;

select * from card_statement_np
where trim(enc_cust_id) in (
    select trim(enc_cust_id) from risk_1410 where 0 < card_bucket union all
    select trim(enc_cust_id) from risk_1411 where 0 < card_bucket union all
    select trim(enc_cust_id) from risk_1412 where 0 < card_bucket
)
;

--針對以上的卡找出與其他商戶的消費狀況 以及是怎樣類型的人
