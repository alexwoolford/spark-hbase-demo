
drop if exists davita_hbase;

create external table davita_hbase(Eid String, dialysis_name string, equip_name string)

ROW FORMAT DELIMITED
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"\
with serdeproperties ("hbase.columns.mapping"=":key,dialysisdetails:name, dialysisdetails:equipment")\
tblproperties("hbase.table.name"="davita_demo");


drop if exists orc_davita;

CREATE TABLE orc_davita STORED AS ORC AS SELECT * FROM davita_hbase;

Select * from orc_davita;