-- Write down the SQL statements you wrote to createing optimized tables
-- and to populate those tables in this file.
-- Remember to add comments explaining why you did so.

-- Task 2:
-- My Optimization for Impala's Schema:
-- 1) Used parquet file format.
-- 2) Partitioned part_opt by p_category for it is a condition of query2.
-- 3) Partitioned supplier_opt by s_city, for it is a condition of query3.
-- 4) Partitioned customer_opt by c_city, for it is a condition of query3.
-- 5) Partitioned dwdate_opt by d_year, for it is a condition of query1 
--    and is used to group by in query2 and query3.
-- 6) Partitioned lineorder_opt by lo_discount, for it is used to search a
--    range in query1.

-- My Optimization for queries:
-- 1) Search the conditions that will remove most rows first, to make the
--    query faster.
-- 2) To prevent OUT OF MEMORY ERROR, put bigger tables earlier.


-- Task 3:
-- 1) Used distribution style for part, supplier, customer, dwdate
-- 2) Used p_brand1 as compound sortkey for part, for it is selected for query1.
-- 3) Used s_city as compound sortkey for supplier, for it is selected and is a
--    condition of query3.
-- 4) Used c_city as compound sortkey for customer, for it is selected and is a
--    condition of query3.
-- 5) Used d_year as compound sortkey for dwdate, for it is used as group by in
--    query2 and query3.
-- 6) Used lo_orderdate, lo_quantity, lo_discount as interleaved sortkey, for
--    they are used in query1 to search for a range.


-- start impala_create_table_opt
CREATE EXTERNAL TABLE part_opt (
	p_partkey INT, 
	p_name STRING, 
	p_mfgr STRING, 
	p_brand1 STRING, 
	p_color STRING, 
	p_type STRING, 
	p_size INT, 
	p_container STRING)
PARTITIONED BY (p_category STRING)
STORED AS PARQUET;

INSERT INTO part_opt PARTITION(p_category)
SELECT p_partkey, p_name, p_mfgr, p_brand1, p_color, p_type, p_size, p_container, p_category
FROM part;

CREATE EXTERNAL TABLE supplier_opt (
	s_suppkey INT,
	s_name STRING,
	s_address STRING,
	s_nation STRING,
	s_region STRING,
	s_phone STRING)
PARTITIONED BY (s_city STRING)
STORED AS PARQUET;

INSERT INTO supplier_opt PARTITION(s_city)
SELECT s_suppkey, s_name, s_address, s_nation, s_region, s_phone, s_city
FROM supplier;

CREATE EXTERNAL TABLE customer_opt (
	c_custkey INT,
	c_name STRING,
	c_address  STRING,
	c_nation STRING,
	c_region STRING,
	c_phone STRING,
	c_mktsegment STRING)
PARTITIONED BY (c_city STRING)
STORED AS PARQUET;

INSERT INTO customer_opt PARTITION(c_city)
SELECT c_custkey, c_name, c_address, c_nation, c_region, c_phone, c_mktsegment, c_city
FROM customer;

CREATE EXTERNAL TABLE dwdate_opt (
	d_datekey INT,
	d_date STRING,
	d_dayofweek STRING,
	d_month STRING,
	d_yearmonthnum INT,
	d_yearmonth STRING,
	d_daynuminweek INT,
	d_daynuminmonth INT,
	d_daynuminyear INT,
	d_monthnuminyear INT,
	d_weeknuminyear INT,
	d_sellingseason STRING,
	d_lastdayinweekfl STRING,
	d_lastdayinmonthfl STRING,
	d_holidayfl STRING,
	d_weekdayfl STRING)
PARTITIONED BY (d_year INT)
STORED AS PARQUET;

INSERT INTO dwdate_opt PARTITION(d_year)
SELECT d_datekey, d_date, d_dayofweek, d_month, d_yearmonthnum, d_yearmonth, d_daynuminweek,
	   d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, 
	   d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl, d_year
FROM dwdate;

CREATE EXTERNAL TABLE lineorder_opt (
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING)
PARTITIONED BY (lo_discount INT)
STORED AS PARQUET;

INSERT INTO lineorder_opt PARTITION(lo_discount)
SELECT lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, 
	   lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_revenue, lo_supplycost, 
	   lo_tax, lo_commitdate, lo_shipmode, lo_discount
FROM lineorder;
-- end impala_create_table_opt