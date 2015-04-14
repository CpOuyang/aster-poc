drop table if exists deposit_transaction;
create table deposit_transaction distribute by hash(this_acct) as
select
    case when 0 < txn_amt then enc_trans_acct_id when txn_amt <= 0 then enc_acct_id end as this_acct,
    case when 0 < txn_amt then enc_acct_id when txn_amt <= 0 then enc_trans_acct_id end as that_acct,
    abs(txn_amt) as txn_amt
from deposit_transaction_1412
where enc_trans_acct_id <> 'BwUBAAUCBnQJAQcIAAdzcyM='
;

--edge account
drop table if exists deposit_account_edge;
create table deposit_account_edge distribute by hash(this_acct) as
select
    this_acct, that_acct, sum(txn_amt) as txn_amt, count(*) as txn_cnt
from deposit_transaction
where 10000 < abs(txn_amt)
group by 1, 2
having 2 < count(*)
;
select count(distinct this_acct), count(distinct that_acct), count(*) from deposit_account_edge;

--node account
drop table if exists deposit_account_node;
create table deposit_account_node distribute by hash(node) as
select this_acct as node from deposit_account_edge union
select that_acct as node from deposit_account_edge
;
select count(*) from deposit_account_node;

--edge customer
drop table if exists deposit_customer_edge;
create table deposit_customer_edge distribute by hash(this_cust) as
select
    b.enc_cust_id as this_cust, c.enc_cust_id as that_cust, sum(txn_amt) as txn_amt, sum(txn_cnt) as txn_cnt
from deposit_account_edge as a
left join bancs_customer_1412 as b on a.this_acct = b.enc_acct_id
left join bancs_customer_1412 as c on a.that_acct = c.enc_acct_id
group by 1, 2
;
select count(distinct this_cust), count(distinct that_cust), count(*) from deposit_customer_edge;





--pagerank_sending
drop table if exists deposit_account_pr_that;
create table deposit_account_pr_that distribute by hash(node) as
select *
from pagerank (
    on deposit_account_node as vertices partition by node 
    on deposit_account_edge as edges partition by this_acct
    targetkey('that_acct')
    accumulate('node')
    edgeweight('txn_amt')
) order by pagerank desc
;
select pagerank, count(*) from deposit_account_pr_that group by 1;

--ntree
drop table if exists deposit_account_nt_input;
create table deposit_account_nt_input distribute by hash(this_acct) as
/*
select * from deposit_account_edge;

insert into deposit_account_nt_input
select '0000' as this_acct,
    a.this_acct as that_acct,
    0.0 as txn_amt,
    0 as txn_cnt
from (select distinct this_acct  from deposit_account_edge) as a
; 
*/
select * from deposit_account_edge
union all
select distinct '0000' as this_acct,
    this_acct as that_acct,
    0.0 as txn_amt,
    0 as txn_cnt
from deposit_account_edge
;

------ntree_lv3.sql-------
drop table if exists deposit_account_nt_lv3;
create table deposit_account_nt_lv3 distribute by hash(id) as
select * from ntree (
    on deposit_account_nt_input
    partition by 1
    root_node(this_acct='0000')
    node_id(that_acct)  
    parent_id(this_acct)
    mode('down')
    allow_cycles('true')
    starts_with('root')
    output('end')
    maxlevel('3')
    result(path(that_acct) as path, is_cycle(*))
)
;
------in linux-------
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv4.sql > ntree_lv4.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv5.sql > ntree_lv5.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv6.sql > ntree_lv6.out &


--Reviewed on 0410


--ntree Lv4

alter table  deposit_account_nt_lv4 add column starter varchar;
update deposit_account_nt_lv4 set starter = split_part(path, '->' , 1);
alter table deposit_account_nt_lv4 add column loop_flag varchar;
update deposit_account_nt_lv4 set loop_flag = '1' where 0 < position(starter in is_cycle);
update deposit_account_nt_lv4 set loop_flag = '0' where 0 = position(starter in is_cycle);
alter table deposit_account_nt_lv4 add column path_len int; 
update deposit_account_nt_lv4 set path_len = length(path)-length(replace(path ,'->',' '));


--ntree,,

drop table if exists co_fund_acct;
CREATE TABLE co_fund_acct distribute BY hash(starter) AS 
SELECT starter , id
FROM  deposit_account_nt_lv4  
where loop_flag = '1' GROUP BY starter , id  
union
SELECT id as starter , starter  as id 
FROM  deposit_account_nt_lv4  
where loop_flag = '1' GROUP BY starter , id  ;


alter table  co_fund_acct  add column max_len int ;
alter table  co_fund_acct  add column co_fund varchar ;

UPDATE co_fund_acct SET  max_len = A.max_len   
FROM  
    (
        SELECT starter , id , MIN(path_len) AS max_len 
        FROM  deposit_account_nt_lv4  
        WHERE loop_flag = '1' GROUP BY starter , id
    ) A
WHERE co_fund_acct.starter = A.starter 
AND co_fund_acct.id = A.id ;

UPDATE co_fund_acct SET  max_len = A.max_len
FROM  
    (
        SELECT starter , id , MIN(path_len) AS max_len 
        FROM co_fund_acct 
        WHERE loop_flag = '1' GROUP BY starter , id
    )A
WHERE co_fund_acct.starter = A.id 
AND co_fund_acct.id = A.starter 
and co_fund_acct.max_len is null ;

UPDATE co_fund_acct SET co_fund ='1';

 
drop table if exists co_fund_acct_communities;
CREATE TABLE co_fund_acct_communities
distribute BY hash(node) AS 
SELECT * FROM CommunityGenerator
( ON co_fund_acct )
PARTITION BY 1
);


--GraphGen
select *
from GraphGen (
    on (select starter, id, 1 as score from co_fund_acct)
    partition BY 1
    SCORE_COL('score')
    ITEM1_COL('starter')
    ITEM2_COL('id')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(1) nodesize_min(1)
    domain('192.168.31.134')
    title('Deposit Transaction')
)
;
--https://192.168.31.134/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_1428609071047_0000_1
/*
library("XML")

a = xmlParse("d:\\sigma_1428609071047_0000_1.gexf")
b = xmlToList(a)

acct_id = NULL
group_id = NULL
for(x in b[["graph"]][["nodes"]]) {
  acct_id = c(acct_id, x$.attrs[2])
  group_id = c(group_id, x[[1]][[2]][2])
}

y = cbind(acct_id, group_id)
rownames(y) = NULL
y = as.data.frame(y)

write.csv(y, "d:\\y.csv")
*/


/*

select c_id, count(*) as cnt,  sum( (case when node like 'O~%'  then 1  else 0  end) ) as cnt_out, sum( (case when node like 'O~%'  then 1  else 0  end) )*1.0/count(*) as out_ratio
from co_fund_1y_50w_communities_cus group by c_id  order by cnt desc ;
*/

--ntree

drop table if exists deposit_account_nt_lv4_min;
create table deposit_account_nt_lv4_min distribute by hash(starter) as
select starter,id,min(path_len) as min_path_len 
from deposit_account_nt_lv4 where loop_flag = '0' group by 1,2;


drop table if exists fund_chain_acct_01;
create table fund_chain_acct_01 distribute by hash(starter) as
select a.* from deposit_account_nt_lv4 a
join deposit_account_nt_lv4_min b 
on a.starter =b.starter  
and a.id =b.id 
and a.path_len=b.min_path_len 
where loop_flag = '0';

drop table if exists fund_chain_acct;
CREATE TABLE fund_chain_acct distribute BY hash(starter) AS 
SELECT * , split_part(path, '->' , 1) as p1 , 
split_part(path, '->' , 2) as p2 ,
split_part(path, '->' , 3) as p3 , 
split_part(path, '->' , 4) as p4
from deposit_account_nt_lv4
where loop_flag = '0' and path_len >=2;   


alter table  fund_chain_acct  add column if_loopin varchar ;
update fund_chain_acct set if_loopin = '1' where 
p1 in(select distinct starter from  co_fund_acct) or
p2 in(select distinct starter from  co_fund_acct) or
p3 in(select distinct starter from  co_fund_acct) or
p4 in(select distinct starter from  co_fund_acct) 
;

/*
select if_loopin,  count(*) from  fund_1y_50w_chain GROUP BY if_loopin;

if_loopin | count(1) 
-----------+----------
           |    52141
 1         | 33500896
(2 rows)
52141
*/


DROP TABLE IF EXISTS fund_chain_acct_all ;

CREATE TABLE fund_chain_acct_all distribute BY hash(this_acct) AS 
SELECT * FROM deposit_account_edge 
WHERE this_acct IN (
SELECT DISTINCT p FROM
(
SELECT p1 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p2 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p3 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p4 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
) A)   AND  that_acct IN (
SELECT DISTINCT p FROM
(
SELECT p1 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p2 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p3 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
UNION
SELECT p4 AS p FROM fund_chain_acct  WHERE if_loopin is NULL
) B)  
;


drop table if exists fund_chain_acct_communities;
CREATE TABLE fund_chain_acct_communities
distribute BY hash(node) AS 
SELECT * FROM CommunityGenerator
( ON fund_chain_acct_all )
PARTITION BY 1
);

--GraphGen
select *
from GraphGen (
    on (select this_acct, that_acct, txn_amt from fund_chain_acct_all)
    partition BY 1
    SCORE_COL('txn_amt')
    ITEM1_COL('this_acct')
    ITEM2_COL('that_acct')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(1) nodesize_min(1)
    domain('192.168.31.134')
    title('Deposit Transaction')
)
;
--https://192.168.31.134/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_1428615716709_0000_1


/*

select c_id, count(*) as cnt,  sum( (case when node like 'O~%'  then 1  else 0  end) ) as cnt_out, sum( (case when node like 'O~%'  then 1  else 0  end) )*1.0/count(*) as out_ratio
from co_fund_1y_50w_communities_cus group by c_id  order by cnt desc ;
*/

SELECT * FROM GraphGen (
on fund_chain_acct_all
PARTITION BY 1
SCORE_COL('txn_amt')
ITEM1_COL('this_acct')
ITEM2_COL('that_acct')
OUTPUT_FORMAT('sigma')
nodesize_max(1) nodesize_min(1) 
directed('true')
domain('192.168.31.134')
title('fund_chain_acct_all')
);




--GraphGen
select *
from GraphGen (
    on (select * from deposit_account_edge where abs(txn_amt) between 200000 and 250000)
    partition BY 1
    SCORE_COL('txn_amt')
    ITEM1_COL('this_acct')
    ITEM2_COL('that_acct')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(1) nodesize_min(1)
    domain('192.168.31.134')
    title('Deposit Transaction')
)
;





grant execute on function pagerank to beehive;

select * from percentile
(on (
    select 1 as grp, *
--    from deposit_transaction_1412
    from card_statement_1412
    where Mcht_id <> '000000000')
partition by grp
percentile('5', '7.5', '10', '75', '80', '85', '90', '95', '97', '98', '99', '100')
target_columns('transaction_amt')
group_columns('grp')
);
 grp | percentile |  txn_amt
-----+------------+------------
   1 |         75 |       7900
   1 |         80 |      12421
   1 |         85 |      21000
   1 |         90 |      40000
   1 |         95 |     101789
   1 |         97 |     242250
   1 |         98 |     460000
   1 |         99 |    1051132
   1 |        100 | 9993627951
(9 rows)

