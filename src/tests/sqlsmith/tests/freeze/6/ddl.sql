CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT (INT '353') AS col_0, t_1.s_suppkey AS col_1, (lower(t_1.s_comment)) AS col_2 FROM nation AS t_0 JOIN supplier AS t_1 ON t_0.n_nationkey = t_1.s_nationkey WHERE false GROUP BY t_1.s_acctbal, t_1.s_suppkey, t_1.s_comment, t_0.n_name;
CREATE MATERIALIZED VIEW m1 AS SELECT t_1.credit_card AS col_0, (REAL '283') AS col_1, t_0.c10 AS col_2, t_1.name AS col_3 FROM alltypes1 AS t_0 LEFT JOIN person AS t_1 ON t_0.c9 = t_1.extra GROUP BY t_1.name, t_0.c6, t_1.extra, t_1.id, t_1.credit_card, t_0.c4, t_0.c15, t_0.c7, t_0.c14, t_0.c10, t_1.date_time, t_1.city HAVING CAST((INT '742') AS BOOLEAN);
CREATE MATERIALIZED VIEW m2 AS SELECT '1z2QocpTXW' AS col_0, t_0.p_brand AS col_1, t_0.p_brand AS col_2 FROM part AS t_0 FULL JOIN m0 AS t_1 ON t_0.p_partkey = t_1.col_1 GROUP BY t_0.p_type, t_0.p_name, t_0.p_brand, t_0.p_retailprice, t_1.col_2, t_0.p_container;
CREATE MATERIALIZED VIEW m3 AS SELECT t_1.c1 AS col_0, t_1.c14 AS col_1 FROM bid AS t_0 FULL JOIN alltypes2 AS t_1 ON t_0.auction = t_1.c4 GROUP BY t_0.auction, t_1.c15, t_1.c1, t_0.url, t_1.c14, t_1.c7 HAVING t_1.c1;
CREATE MATERIALIZED VIEW m4 AS SELECT t_0.c_custkey AS col_0, t_1.o_clerk AS col_1, (INT '0') AS col_2, t_1.o_totalprice AS col_3 FROM customer AS t_0 JOIN orders AS t_1 ON t_0.c_custkey = t_1.o_custkey WHERE (t_0.c_phone > (md5((TRIM(LEADING 'LbH8Y1wzBw' FROM t_0.c_comment))))) GROUP BY t_1.o_orderstatus, t_1.o_shippriority, t_0.c_custkey, t_1.o_totalprice, t_1.o_comment, t_0.c_phone, t_0.c_comment, t_1.o_custkey, t_1.o_clerk;
CREATE MATERIALIZED VIEW m5 AS WITH with_0 AS (SELECT ((SMALLINT '566') # t_1.col_2) AS col_0, t_2.state AS col_1 FROM m4 AS t_1 LEFT JOIN person AS t_2 ON t_1.col_1 = t_2.city GROUP BY t_1.col_2, t_2.city, t_2.state, t_2.credit_card, t_1.col_3) SELECT TIMESTAMP '2022-11-11 11:43:30' AS col_0, (BIGINT '721') AS col_1 FROM with_0;
CREATE MATERIALIZED VIEW m6 AS SELECT t_0.c_acctbal AS col_0 FROM customer AS t_0 GROUP BY t_0.c_phone, t_0.c_nationkey, t_0.c_acctbal;
CREATE MATERIALIZED VIEW m8 AS SELECT (DATE '2022-11-09' - min(CAST(hop_0.c1 AS INT)) FILTER(WHERE true)) AS col_0, TIMESTAMP '2022-11-11 11:43:32' AS col_1, hop_0.c15 AS col_2, hop_0.c15 AS col_3 FROM hop(alltypes1, alltypes1.c11, INTERVAL '604800', INTERVAL '50803200') AS hop_0 GROUP BY hop_0.c15 HAVING false;
CREATE MATERIALIZED VIEW m9 AS SELECT t_0.col_1 AS col_0 FROM m4 AS t_0 GROUP BY t_0.col_1;