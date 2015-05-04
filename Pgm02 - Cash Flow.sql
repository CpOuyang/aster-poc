drop table if exists deposit_transaction;
create table deposit_transaction distribute by hash(this_acct) as
select
    case when 0 < txn_amt then trim(enc_trans_acct_id) when txn_amt <= 0 then trim(enc_acct_id) end as this_acct,
    case when 0 < txn_amt then trim(enc_acct_id) when txn_amt <= 0 then trim(enc_trans_acct_id) end as that_acct,
    abs(txn_amt) as txn_amt
from (
    select *, '20141001'::timestamp as Snapshot from deposit_transaction_1410 union all
    select *, '20141101'::timestamp as Snapshot from deposit_transaction_1411 union all
    select *, '20141201'::timestamp as Snapshot from deposit_transaction_1412
) as a
where trim(enc_trans_acct_id) <> 'BwUBAAUCBnQJAQcIAAdzcyM='
;
select count(*) from deposit_transaction;

--edge account
drop table if exists deposit_account_edge;
create table deposit_account_edge distribute by hash(this_acct) as
select
    this_acct, that_acct, sum(txn_amt) as txn_amt, count(*) as txn_cnt
from deposit_transaction
where true
    and 30000 < abs(txn_amt)
    and this_acct <> that_acct --存款機
    -- and (trim(this_acct) in (select trim(enc_acct_id) from merchant_profile_1412) or
        -- trim(that_acct) in (select trim(enc_acct_id) from merchant_profile_1412))
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


--edge merchant
drop table if exists deposit_merchant_edge;
create table deposit_merchant_edge distribute by hash(this_acct) as
select *
from deposit_account_edge
where true
    and trim(this_acct) in (select trim(enc_acct_id) from merchant_profile_1412)
    and trim(that_acct) in (select trim(enc_acct_id) from merchant_profile_1412)
;
select count(distinct this_acct), count(distinct that_acct), count(*) from deposit_merchant_edge;

--node merchant
drop table if exists deposit_merchant_node;
create table deposit_merchant_node distribute by hash(node) as
select trim(this_acct) as node from deposit_merchant_edge union
select trim(that_acct) as node from deposit_merchant_edge
;
select count(*) from deposit_merchant_node;


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



------ntree_c5.sql-------
drop table if exists deposit_merchant_nt5;
create table deposit_merchant_nt5 distribute by hash(id) as
select * from ntree (
    on (select * from deposit_merchant_edge
        union all
        select distinct '0000', this_acct, 0.0, 0 from deposit_merchant_edge)
    partition by 1
    root_node(this_acct = '0000')
    node_id(that_acct)
    parent_id(this_acct)
    mode('down')
    allow_cycles('true')
    starts_with('root')
    output('end')
    maxlevel('5')
    result(path(that_acct) as path, is_cycle(*))
)
;
------batch in background-------
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_c5.sql > ntree_c5.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_c5_c4_a4.sql > ntree_c5_c4_a4.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f ntree_c5_a4_c6.sql > ntree_c5_a4_c6.out &
nohup act -d beehive -h 192.168.31.134 -U beehive -w beehive -f onetime.sql > onetime.out &



--ntree analysis
alter table  deposit_merchant_nt5 add column starter varchar;
update deposit_merchant_nt5 set starter = split_part(path, '->', 1);

alter table deposit_merchant_nt5 add column loop_flag varchar;
update deposit_merchant_nt5 set loop_flag = '1' where 0 < position(starter in is_cycle);
update deposit_merchant_nt5 set loop_flag = '0' where 0 = position(starter in is_cycle);

alter table deposit_merchant_nt5 add column path_len int; 
update deposit_merchant_nt5 set path_len = length(path) - length(replace(path, '->', ' '));


drop table if exists deposit_merchant_nt5_cofund;
create table deposit_merchant_nt5_cofund distribute by hash(starter) as
select starter, id
from  deposit_merchant_nt5
where loop_flag = '1'
union
select id as starter, starter as id
from  deposit_merchant_nt5
where loop_flag = '1'
;
select count(*) from deposit_merchant_nt5_cofund;

alter table  deposit_merchant_nt5_cofund  add column max_len int; --最短路徑?
alter table  deposit_merchant_nt5_cofund  add column co_fund varchar;


--???
update deposit_merchant_nt5_cofund
set max_len = a.max_len
from (
    select starter, id, min(path_len) as max_len --starter -> A -> B -> ... -> Z -> id (-> starter)
    from  deposit_merchant_nt5                --id is the node about to close the loop (go to starter)
    where loop_flag = '1'
    group by starter, id
) as a
where deposit_merchant_nt5_cofund.starter = a.starter
    and deposit_merchant_nt5_cofund.id = a.id
;

update deposit_merchant_nt5_cofund
set max_len = a.max_len
from (
    select starter, id, min(path_len) as max_len
    from deposit_merchant_nt5
    where loop_flag = '1'
    group by starter, id
) as a
where deposit_merchant_nt5_cofund.starter = a.id
    and deposit_merchant_nt5_cofund.id = a.starter 
    and deposit_merchant_nt5_cofund.max_len is null
;
update deposit_merchant_nt5_cofund set co_fund ='1';

 
/*
drop table if exists deposit_customer_nt5_cofund_communities;
CREATE TABLE deposit_customer_nt5_cofund_communities
distribute BY hash(node) AS 
SELECT * FROM CommunityGenerator
( ON deposit_customer_nt5_cofund )
PARTITION BY 1
);
*/

--GraphGen
select *
from GraphGen (
    on (select starter, id, 1 as score from deposit_merchant_nt5_cofund)
    partition by 1
    score_col('score')
    item1_col('starter')
    item2_col('id')
    output_format('sigma')
    directed('true')
    nodesize_max(1)
    nodesize_min(1)
    domain('192.168.31.134')
    title('Cofunds Community')
)
;
-- https://192.168.31.134/chrysalis/mr/graphgen/sigma/sigma.html?id=sigma_143023776225421

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

-- drop table if exists deposit_customer_nt5_min;
-- create table deposit_customer_nt5_min distribute by hash(starter) as
-- select starter,id,min(path_len) as min_path_len 
-- from deposit_customer_nt5 where loop_flag = '0' group by 1,2;

-- drop table if exists fund_chain_cust_01;
-- create table fund_chain_cust_01 distribute by hash(starter) as
-- select a.* from deposit_customer_nt5 a
-- join deposit_customer_nt5_min b 
-- on a.starter =b.starter  
-- and a.id =b.id 
-- and a.path_len=b.min_path_len 
-- where loop_flag = '0';

/*
drop table if exists fund_chain_cust;
create table fund_chain_cust distribute by hash(starter) as 
select *,
    split_part(path, '->', 1) as p1,
    split_part(path, '->', 2) as p2,
    split_part(path, '->', 3) as p3,
    split_part(path, '->', 4) as p4,
    split_part(path, '->', 5) as p5
from deposit_customer_nt5
where loop_flag = '0'
    and 2 <= path_len
;

alter table  fund_chain_cust add column if_loopin varchar;
update fund_chain_cust set if_loopin = '1'
where false or
    p1 in (select distinct starter from deposit_customer_nt5_cofund) or
    p2 in (select distinct starter from deposit_customer_nt5_cofund) or
    p3 in (select distinct starter from deposit_customer_nt5_cofund) or
    p4 in (select distinct starter from deposit_customer_nt5_cofund) or
    p5 in (select distinct starter from deposit_customer_nt5_cofund)
;


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
*/

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
    on (select this_acct, that_acct, txn_amt
        from deposit_merchant_edge
        where true and
            trim(this_acct) in (
                select trim(p)
                from (
                    select split_part(path, '->', 1) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 2) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 3) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 4) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 5) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len) as a
                where trim(p) not in (select trim(starter) from deposit_merchant_nt5_cofund))
            and trim(that_acct) in (
                select trim(p)
                from (
                    select split_part(path, '->', 1) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 2) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 3) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 4) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len union all
                    select split_part(path, '->', 5) as p from deposit_merchant_nt5 where loop_flag = '0' and 2 <= path_len) as a
                where trim(p) not in (select trim(starter) from deposit_merchant_nt5_cofund))
    ) as data
    partition by 1
-- only required for additional information
    on (select a.enc_acct_id,
            a.corp_id,
            b.register_name
        from merchant_profile_1412 as a
        left join merchant_application as b on trim(a.mcht_id) = trim(b.mcht_id)
        group by 1, 2, 3) as nodes dimension
    item_join_col('enc_acct_id')
--
    score_col('txn_amt')
    item1_col('this_acct')
    item2_col('that_acct')
    output_format('sigma')
    directed('true')
    nodesize_max(1)
    nodesize_min(1)
    domain('192.168.31.134')
    title('Sequential Community')
)
;
--



--Analytics
drop table if exists deposit_customer_analytics;
create table deposit_customer_analytics distribute by hash(enc_cust_id) as
select a.node as Enc_Cust_Id,
    b.pagerank,
    c.modularity as Mod_Cofund,
    d.modularity as Mod_Sequential,
    e.card_bucket,
    e.loan_bucket,
    e.cust_bucket,
    e.TAS,
    e.TBS,
    e.TCS
from deposit_customer_node as a
left join deposit_customer_pr_that as b on trim(a.node) = trim(b.node)
left join gg_cofund as c on trim(a.node) = trim(c.enc_cust_id)
left join gg_sequential as d on trim(a.node) = trim(d.enc_cust_id)
left join (
    select * from (
        select
            *, row_number() over (partition by enc_cust_id order by as_of_date desc) as sn
        from (
            select *, '20141001'::date as as_of_date from risk_1410 union all
            select *, '20141101'::date as as_of_date from risk_1411 union all
            select *, '20141201'::date as as_of_date from risk_1412
        ) as x
    ) as y
    where sn = 1
) as e on trim(a.node) = trim(e.enc_cust_id)
;
select * from deposit_customer_analytics;



-- merchant cash-flow
select *
from GraphGen (
    on (select * from deposit_account_edge
        where trim(this_acct) in (select trim(enc_acct_id) from merchant_profile_1412)
            and trim(that_acct) in (select trim(enc_acct_id) from merchant_profile_1412)) as data
    partition by 1
    on (select a.enc_acct_id,
            a.corp_id,
            b.register_name
        from merchant_profile_1412 as a
        left join merchant_application as b on trim(a.mcht_id) = trim(b.mcht_id)
        group by 1, 2, 3) as nodes dimension
    item_join_col('enc_acct_id')
    score_col('txn_amt')
    item1_col('this_acct')
    item2_col('that_acct')
    output_format('sigma')
    directed('true')
    nodesize_max(1)
    nodesize_min(1)
    domain('192.168.31.134')
    title('merchant transaction')
)
;
