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
CREATE MATERIALIZED VIEW m0 AS WITH with_0 AS (SELECT t_1.c3 AS col_0, CAST(t_2.c1 AS INT) AS col_1 FROM alltypes2 AS t_1 FULL JOIN alltypes1 AS t_2 ON t_1.c13 = t_2.c13 AND t_2.c1 WHERE t_1.c1 GROUP BY t_1.c3, t_2.c6, t_2.c5, t_2.c2, t_2.c3, t_1.c5, t_2.c9, t_2.c11, t_2.c1, t_1.c11, t_1.c2, t_1.c7, t_2.c10, t_2.c13, t_1.c4) SELECT (DATE '2022-07-10' + (INTERVAL '-722661')) AS col_0, (936) AS col_1, DATE '2022-07-10' AS col_2 FROM with_0;
CREATE MATERIALIZED VIEW m3 AS SELECT hop_0.date_time AS col_0 FROM hop(person, person.date_time, INTERVAL '60', INTERVAL '5700') AS hop_0 GROUP BY hop_0.state, hop_0.date_time;
CREATE MATERIALIZED VIEW m4 AS WITH with_0 AS (SELECT sq_6.col_0 AS col_0, (FLOAT '431') AS col_1, ((FLOAT '762')) AS col_2 FROM (WITH with_1 AS (SELECT sq_5.col_0 AS col_0, sq_5.col_0 AS col_1 FROM (SELECT (- sq_4.col_0) AS col_0 FROM (SELECT (SMALLINT '842') AS col_0 FROM m0 AS t_2 LEFT JOIN customer AS t_3 ON t_2.col_1 = t_3.c_acctbal AND true GROUP BY t_3.c_address, t_3.c_custkey HAVING false) AS sq_4 GROUP BY sq_4.col_0) AS sq_5 GROUP BY sq_5.col_0 HAVING true) SELECT (FLOAT '502') AS col_0, (REAL '-1394747370') AS col_1, 'SBDHrZSynr' AS col_2, (BIGINT '141') AS col_3 FROM with_1 WHERE true) AS sq_6 WHERE false GROUP BY sq_6.col_0) SELECT min(TIMESTAMP '2022-07-05 03:02:58') AS col_0, CAST(NULL AS STRUCT<a TIME, b INT>) AS col_1, true AS col_2, (BIGINT '251') AS col_3 FROM with_0;
CREATE MATERIALIZED VIEW m5 AS SELECT (OVERLAY(t_0.item_name PLACING '6N6Bf4QX8t' FROM t_1.n_nationkey FOR (coalesce(NULL, (INT '743'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)))) AS col_0, (OVERLAY('2Yue7vtrJt' PLACING t_1.n_comment FROM t_1.n_nationkey)) AS col_1, t_0.description AS col_2, ((t_0.id / (SMALLINT '956')) - t_1.n_nationkey) AS col_3 FROM auction AS t_0 RIGHT JOIN nation AS t_1 ON t_0.extra = t_1.n_comment GROUP BY t_1.n_comment, t_1.n_nationkey, t_0.id, t_0.seller, t_0.description, t_0.item_name HAVING (((REAL '359') + (REAL '966')) = (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, t_1.n_nationkey)));
CREATE MATERIALIZED VIEW m6 AS SELECT hop_0.col_3 AS col_0 FROM hop(m4, m4.col_0, INTERVAL '86400', INTERVAL '1728000') AS hop_0 GROUP BY hop_0.col_3 HAVING true;
CREATE MATERIALIZED VIEW m7 AS SELECT tumble_0.auction AS col_0 FROM tumble(bid, bid.date_time, INTERVAL '50') AS tumble_0 WHERE (coalesce(NULL, NULL, NULL, true, NULL, NULL, NULL, NULL, NULL, NULL)) GROUP BY tumble_0.auction HAVING CAST((INT '922') AS BOOLEAN);
CREATE MATERIALIZED VIEW m8 AS SELECT hop_0.id AS col_0, hop_0.seller AS col_1 FROM hop(auction, auction.date_time, INTERVAL '511416', INTERVAL '35287704') AS hop_0 WHERE true GROUP BY hop_0.seller, hop_0.id, hop_0.item_name HAVING true;
CREATE MATERIALIZED VIEW m9 AS SELECT t_1.r_regionkey AS col_0, t_1.r_regionkey AS col_1, (OVERLAY('JIKjeJvzgx' PLACING (coalesce(t_1.r_comment, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) FROM (t_1.r_regionkey << (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (SMALLINT '792')))) FOR t_1.r_regionkey)) AS col_2 FROM person AS t_0 FULL JOIN region AS t_1 ON t_0.city = t_1.r_name WHERE true GROUP BY t_1.r_comment, t_1.r_regionkey HAVING (false);