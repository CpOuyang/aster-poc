drop table if exists deposit_transaction;
create table deposit_transaction distribute by hash(this_acct) as
select
    case when 0 < txn_amt then enc_trans_acct_id when txn_amt <= 0 then enc_acct_id end as this_acct,
    case when 0 < txn_amt then enc_acct_id when txn_amt <= 0 then enc_trans_acct_id end as that_acct,
    abs(txn_amt) as txn_amt
from (
    select *, '20141001'::timestamp as Snapshot from deposit_transaction_1410 union all
    select *, '20141101'::timestamp as Snapshot from deposit_transaction_1411 union all
    select *, '20141201'::timestamp as Snapshot from deposit_transaction_1412
) as a
where enc_trans_acct_id <> 'BwUBAAUCBnQJAQcIAAdzcyM='
;
select count(*) from deposit_transaction;

--edge account
drop table if exists deposit_account_edge;
create table deposit_account_edge distribute by hash(this_acct) as
select
    this_acct, that_acct, sum(txn_amt) as txn_amt, count(*) as txn_cnt
from deposit_transaction
where true --rewrite
    and 30000 < abs(txn_amt)
    and this_acct <> that_acct --存款機交易
group by 1, 2
;
select count(distinct this_acct), count(distinct that_acct), count(*) from deposit_account_edge;

--node account
drop table if exists deposit_account_node;
create table deposit_account_node distribute by hash(node) as
select this_acct as node from deposit_account_edge union
select that_acct as node from deposit_account_edge
;
select count(*) from deposit_account_node;

--account linkage
drop table if exists deposit_account_link;
create table deposit_account_link distribute by hash(this_acct) as
select a.*,
    trim(b.enc_cust_id) as this_cust,
    trim(c.enc_cust_id) as that_cust
from deposit_account_edge as a
left join (
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1410 union
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1411 union
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1412
) as b on trim(a.this_acct) = trim(b.enc_acct_id)
left join (
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1410 union
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1411 union
    select trim(enc_acct_id) as Enc_Acct_Id, trim(enc_cust_id) as Enc_Cust_Id from deposit_1412
) as c on trim(a.that_acct) = trim(c.enc_acct_id)
;

--edge customer
drop table if exists deposit_customer_edge;
create table deposit_customer_edge distribute by hash(this_cust) as
select this_cust,
    that_cust,
    sum(txn_amt) as txn_amt,
    sum(txn_cnt) as txn_cnt
from deposit_account_link
where not (this_cust is null or that_cust is null)
    and this_cust <> that_cust --左手給右手
group by 1, 2
;
select count(distinct this_cust), count(distinct that_cust), count(*) from deposit_customer_edge;

--node customer
drop table if exists deposit_customer_node;
create table deposit_customer_node distribute by hash(node) as
select this_cust as node from deposit_customer_edge union
select that_cust as node from deposit_customer_edge
;
select count(*) from deposit_customer_node;



--pagerank sending
drop table if exists deposit_customer_pr_that;
create table deposit_customer_pr_that distribute by hash(node) as
select *
from pagerank (
    on deposit_customer_node as vertices partition by node 
    on deposit_customer_edge as edges partition by this_cust
    targetkey('that_cust')
    accumulate('node')
    edgeweight('txn_amt')
) order by pagerank desc
;
select pagerank, count(*) from deposit_customer_pr_that group by 1;

--betweenness sending
drop table if exists deposit_customer_bt_that;
create table deposit_customer_bt_that distribute by hash(node) as
select *
from betweenness (
    on deposit_customer_node as vertices partition by node 
    on deposit_customer_edge as edges partition by this_cust
    targetkey('that_cust')
    directed('t')
    accumulate('node')
--    edgeweight('txn_amt')
) order by betweenness desc
;
select betweenness, count(*) from deposit_customer_bt_that group by 1;

--LocalClusteringCoefficient sending
drop table if exists deposit_customer_cc_that;
create table deposit_customer_cc_that distribute by hash(node) as
select *
from LocalClusteringCoefficient (
    on deposit_customer_node as vertices partition by node 
    on deposit_customer_edge as edges partition by this_cust
    targetkey('that_cust')
    directed('t')
    accumulate('node')
--    edgeweight('txn_amt')
) order by avg_cc desc
;
select avg_cc, count(*) from deposit_customer_cc_that group by 1;



----ntree preparing
--drop table if exists deposit_account_nt_input;
--create table deposit_account_nt_input distribute by hash(this_acct) as
--select * from deposit_account_edge
--union all
--select distinct '0000' as this_acct,
--    this_acct as that_acct,
--    0.0 as txn_amt,
--    0 as txn_cnt
--from deposit_account_edge
--;

------ntree_lv3.sql-------
drop table if exists deposit_account_nt_lv3;
create table deposit_account_nt_lv3 distribute by hash(id) as
select * from ntree (
--    on deposit_account_nt_input
    on (select * from deposit_account_edge
        where 500000 <= txn_amt
        union all
        select distinct '0000', this_acct, 0.0, 0 from deposit_account_edge
        where 500000 <= txn_amt)
--    on deposit_account_edge
    partition by 1
    root_node(this_acct='0000')
--    root_node(true)
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
------batch in background-------
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv3.sql > ntree_lv3.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv4.sql > ntree_lv4.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv5.sql > ntree_lv5.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_lv6.sql > ntree_lv6.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_c5_c4_a4.sql > ntree_c5_c4_a4.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_c5_a4_c6.sql > ntree_c5_a4_c6.out &



--ntree analysis
alter table  deposit_customer_nt_lv5 add column starter varchar;
update deposit_customer_nt_lv5 set starter = split_part(path, '->', 1);

alter table deposit_customer_nt_lv5 add column loop_flag varchar;
update deposit_customer_nt_lv5 set loop_flag = '1' where 0 < position(starter in is_cycle);
update deposit_customer_nt_lv5 set loop_flag = '0' where 0 = position(starter in is_cycle);

alter table deposit_customer_nt_lv5 add column path_len int; 
update deposit_customer_nt_lv5 set path_len = length(path) - length(replace(path, '->', ' '));


drop table if exists deposit_customer_nt_cofund;
create table deposit_customer_nt_cofund distribute by hash(starter) as
select starter, id
from  deposit_customer_nt_lv5
where loop_flag = '1'
union
select id as starter, starter as id
from  deposit_customer_nt_lv5
where loop_flag = '1'
;
select count(*) from deposit_customer_nt_cofund;

alter table  deposit_customer_nt_cofund  add column max_len int;
alter table  deposit_customer_nt_cofund  add column co_fund varchar;

--???
update deposit_customer_nt_cofund
set max_len = a.max_len
from (
    select starter, id, min(path_len) as max_len
    from  deposit_customer_nt_lv5
    where loop_flag = '1'
    group by starter, id
) as a
where deposit_customer_nt_cofund.starter = a.starter
    and deposit_customer_nt_cofund.id = a.id
;

update deposit_customer_nt_cofund
set max_len = a.max_len
from (
    select starter, id, min(path_len) as max_len
    from deposit_customer_nt_lv5
    where loop_flag = '1'
    group by starter, id
) as a
where deposit_customer_nt_cofund.starter = a.id
    and deposit_customer_nt_cofund.id = a.starter 
    and deposit_customer_nt_cofund.max_len is null
;
update deposit_customer_nt_cofund set co_fund ='1';

 
/*
drop table if exists deposit_customer_nt_cofund_communities;
CREATE TABLE deposit_customer_nt_cofund_communities
distribute BY hash(node) AS 
SELECT * FROM CommunityGenerator
( ON deposit_customer_nt_cofund )
PARTITION BY 1
);
*/

--GraphGen
select *
from GraphGen (
    on (select starter, id, 1 as score from deposit_customer_nt_cofund)
    partition BY 1
    SCORE_COL('score')
    ITEM1_COL('starter')
    ITEM2_COL('id')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(1) nodesize_min(1)
    domain('192.168.31.134')
    title('Cofunds Community')
)
;
--https://192.168.31.134/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_1429196992216_0002_1
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
from co_fund_1y_50w_communities_cus group by c_id  order by cnt desc;
*/

--ntree

drop table if exists deposit_customer_nt_lv5_min;
create table deposit_customer_nt_lv5_min distribute by hash(starter) as
select starter,id,min(path_len) as min_path_len 
from deposit_customer_nt_lv5 where loop_flag = '0' group by 1,2;

drop table if exists fund_chain_cust_01;
create table fund_chain_cust_01 distribute by hash(starter) as
select a.* from deposit_customer_nt_lv5 a
join deposit_customer_nt_lv5_min b 
on a.starter =b.starter  
and a.id =b.id 
and a.path_len=b.min_path_len 
where loop_flag = '0';

drop table if exists fund_chain_cust;
CREATE TABLE fund_chain_cust distribute BY hash(starter) AS 
SELECT *, split_part(path, '->', 1) as p1, 
split_part(path, '->', 2) as p2,
split_part(path, '->', 3) as p3, 
split_part(path, '->', 4) as p4
from deposit_customer_nt_lv5
where loop_flag = '0' and path_len >=2;   


alter table  fund_chain_cust  add column if_loopin varchar;
update fund_chain_cust set if_loopin = '1' where 
p1 in(select distinct starter from  deposit_customer_nt_cofund) or
p2 in(select distinct starter from  deposit_customer_nt_cofund) or
p3 in(select distinct starter from  deposit_customer_nt_cofund) or
p4 in(select distinct starter from  deposit_customer_nt_cofund) 
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


DROP TABLE IF EXISTS fund_chain_cust_all;

CREATE TABLE fund_chain_cust_all distribute BY hash(this_cust) AS 
SELECT * FROM deposit_customer_edge 
WHERE this_cust IN (
SELECT DISTINCT p FROM
(
SELECT p1 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p2 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p3 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p4 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
) A)   AND  that_cust IN (
SELECT DISTINCT p FROM
(
SELECT p1 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p2 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p3 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
UNION
SELECT p4 AS p FROM fund_chain_cust  WHERE if_loopin is NULL
) B)  
;

/*
drop table if exists fund_chain_acct_communities;
CREATE TABLE fund_chain_acct_communities
distribute BY hash(node) AS 
SELECT * FROM CommunityGenerator
( ON fund_chain_acct_all )
PARTITION BY 1
);
*/

--GraphGen
select *
from GraphGen (
    on (select this_cust, that_cust, txn_amt from fund_chain_cust_all)
    partition BY 1
    SCORE_COL('txn_amt')
    ITEM1_COL('this_cust')
    ITEM2_COL('that_cust')
    OUTPUT_FORMAT('sigma')
    directed('true')
    nodesize_max(1) nodesize_min(1)
    domain('192.168.31.134')
    title('Sequential Cofunds')
)
;
--https://192.168.31.134/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_1428615716709_0000_1


/*

select c_id, count(*) as cnt,  sum( (case when node like 'O~%'  then 1  else 0  end) ) as cnt_out, sum( (case when node like 'O~%'  then 1  else 0  end) )*1.0/count(*) as out_ratio
from co_fund_1y_50w_communities_cus group by c_id  order by cnt desc;
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
