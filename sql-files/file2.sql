DROP this_table;

create table this_new_table as
SELECT * from
hive.aggregate_pa.x_merchant_fact_new_intermediate;
