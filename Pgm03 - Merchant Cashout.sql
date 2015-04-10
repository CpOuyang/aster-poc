
--0.把原始數據先做篩選(Mcht_Id為0的暫刪除,大額門檻)

card_statement_1412 -> pos_txn

drop table if exists pos_txn;
create table pos_txn distribute by hash(Enc_Acct_Id) as
select * from card_statement_1412
where Mcht_Id <> '000000000' and  5000 < Transaction_Amt
and Transaction_City <> '';

select count(*) from pos_txn;
--772,997


--1. 數據準備 **先把交易日期轉成time_stamp
drop table if exists pos_txn_nl;
create table pos_txn_nl distribute by hash(enc_acct_id) as
select 
    Enc_Acct_Id,
    Mcht_Id,
    Currency_Code,
    Currency_Transaction_Amt,
    Mcc_Code,
    Microfilm_Id,
    Pos_Entry_Mode,
    Product_Id,
    Statement_Date,
    Statement_Sequence_Nbr,
    Transaction_Amt,
    Transaction_City,
    Transaction_Code,
    Transaction_Country_Code,
    (Transaction_date::timestamp) as Transaction_Date,
    Transaction_Desc_Chinese,
    Transaction_Posting_Date
from pos_txn;



--2.數據分析

--利用sessionize識別刷卡週期性與短時間內大額消費
 
drop table if exists pos_card_session;
create table pos_card_session distribute by hash(enc_acct_id) as
SELECT *
FROM SESSIONIZE (
    ON pos_txn_nl
    PARTITION BY Enc_Acct_Id,Mcht_Id
    ORDER BY transaction_date
    TIMECOLUMN ('transaction_date')
    TIMEOUT ('2592000') --30天算1個session
    RAPIDFIRE ('86399') --1天有多筆就代表有问题
--    RAPIDFIRE ('86401') --1天有多筆就代表有问题
);
/*
select * from pos_card_session
where enc_acct_id like '%AwUBAwMDB3wJAQcJCAB1dCM%'
    and mcht_id like '%000000359%'
order by transaction_date
;
*/

--sessionize結果統計

drop table if exists pos_card_session_statis;
create table pos_card_session_statis distribute by hash(Enc_Acct_Id) as
select A1.Enc_Acct_Id,
       A1.session_cnt+1 as session_cnt , 
       A1.rapidfire_cnt_24h
from 
  (
  select Enc_Acct_Id,
    max(sessionid) as session_cnt,
    sum ( case when rapidfire = 'true' then 1 else 0 end  ) as rapidfire_cnt_24h
  from pos_card_session
  group by Enc_Acct_Id 
  ) A1;
  
      
      
       
--利用npath識別信用卡是否都在同一家商戶消費

drop table if exists pos_path_mid;
create table pos_path_mid distribute by hash(Enc_Acct_Id) as
select * , extract(day from (end_tran_dt - BEGIN_tran_dt)) as duration_day
FROM NPATH (
    ON pos_txn_nl
    PARTITION BY  Enc_Acct_Id,Mcht_Id
    order by Transaction_Date 
    PATTERN('A.(B|C)*')
    MODE(NONOVERLAPPING)
    SYMBOLS
    (
    TRUE AS A,
    mcht_id !=lag(Mcht_Id,1) as B ,
    mcht_id  =lag(Mcht_Id,1) as C 
    )
            result
            (
            first(Transaction_Date of any(A,B,C)) AS BEGIN_tran_dt,
            last(Transaction_Date of any(A,B,C)) as end_tran_dt,
          count(Transaction_Amt OF any(A,B,C)) as txn_cnt,  -- 总 笔数
            first(Enc_Acct_Id of any(A,B,C)) as Enc_Acct_Id,
            SUM(Transaction_Amt OF any(A,B,C)) as txn_amt_sum,
            accumulate(Mcht_Id of any(A,B,C)) as mid_path,
            count(Mcht_Id of B) as mid_change_cnt,
            count(Mcht_Id of C) as mid_nochange_cnt,
            accumulate(Mcht_Id  OF C) as mid_dup_path 
            )
);  




--識別商戶與信用卡有無頻繁週期性大額消費的關係

drop table if exists mid_card_cashout_relation;
create table mid_card_cashout_relation distribute by hash(Enc_Acct_Id) as
SELECT Enc_Acct_Id , ngram as mid , frequency  as frequency
FROM nGram (
ON (
select	Enc_Acct_Id ,
				substring(mid_dup_path from 2 for length(mid_dup_path)-2) --只取[]内的
        as mid_path 
from pos_path_mid where mid_dup_path != '[]' 
 	 ) 
TEXT_COLUMN('mid_path')
DELIMITER(',')
GRAMS(1)
OVERLAPPING('false')
CASE_INSENSITIVE('true')
PUNCTUATION('\[.,?\!\]')
RESET('\[.,?\!\]')
ACCUMULATE('enc_acct_id'))
ORDER BY Enc_Acct_Id;




/*
--npath其他練習
drop table if exists pos_npath_5min_rslt;
create table pos_npath_5min_rslt distribute by hash(card_no1) as
select card_no1,mid,begin_tran_tm,end_tran_tm,b_cnt+1 as txn_cnt,
a_amt+b_amt as txn_amt,accum_txn_amt
FROM NPATH(
ON 
(
	select distinct * from pos_txn_nl
	where tranamt>=10000
	and (trankind ='071:贷记卡消费' or trankind ='071:银联卡消费')		
)
PARTITION BY card_no1
order by tran_tm 
PATTERN('A.B+')
MODE(NONOVERLAPPING)
SYMBOLS
(
TRUE AS A , --这次套现的第一笔
tran_tm -lag(tran_tm,1)<='000500'::time AS B --只有的每笔都是同一个商户且与上笔套现相隔时间较短,这里只关心五分钟内连续刷卡行为
)
		result
		(
		first(tran_tm of A) AS BEGIN_tran_tm,
		last(tran_tm of B) as end_tran_tm,
	  count(tranamt OF B) as b_cnt,  --b_cnt+1就是总套现笔数
		first(card_no1 of A) as card_no1,
		first(tranamt of A) as a_amt,
		SUM(tranamt OF B) as b_amt,
		first(mid of A) as mid,
		accumulate(tranamt  OF any (A,B)) as accum_txn_amt
		)
)
;        
*/ 

 	
--以上嫌疑特徵統整

drop table if exists pos_card_cashout_view;
create table pos_card_cashout_view distribute by hash(Enc_Acct_Id) as
 
select 	A3.Enc_Acct_Id,
        A3.session_cnt,
        A3.rapidfire_cnt_24h, 
        A6.BEGIN_tran_dt,
        A6.end_tran_dt,
        A6.txn_cnt,
        A6.txn_amt,
        A6.mid_path,
        A6.mid_change_cnt,
        A6.mid_nochange_cnt,
        A6.mid_dup_path, 
        A6.duration_day,
		    A7.num_mid,  
		    A7.cashout_cnt_8000
from 
left join  pos_path_mid  A6
on A3.Enc_Acct_Id = A6.Enc_Acct_Id
left join 
(select Enc_Acct_Id, count(*) as num_mid,  sum(frequency) as cashout_cnt  
from mid_card_cashout_relation group by Enc_Acct_Id) A7
on A3.Enc_Acct_Id = A7.Enc_Acct_Id;
 

--視覺化 
select *
FROM GraphGen (
    on (select Enc_Acct_Id, 'M~' ||  mid as mid, frequency  from mid_card_cashout_relation where frequency > 5) 
    PARTITION BY 1
    SCORE_COL('frequency')
    ITEM1_COL('enc_acct_id')
    ITEM2_COL('mid')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(5) nodesize_min(1)
    domain('192.168.20.129')
    title('cashout_direction frequency >5 ')
);
--https://192.168.20.129/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_1428682011948_0000_1

 
drop table if exists pos_path_mid_path;
create table pos_path_mid_path distribute by hash(mid_path) as
select mid_path,count(*) as cnt from pos_path_mid 
where mid_path<>'' group by 1;

select *
FROM GraphGen (
on (select * from pos_path_mid_path where cnt>4) 
PARTITION BY 1
order by cnt desc
item_format('npath')
item1_col('mid_path')
score_col('cnt')
output_format('sankey')
nodesize_max(5) nodesize_min(1)
domain('192.168.20.129')
title('mid_path')
);
--https://192.168.20.129/chrysalis/mr/graphgen/sankey/sankey_1428682209819_0000_1.html
