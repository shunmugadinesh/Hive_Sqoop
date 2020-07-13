sqooptohiveload.sh 		- Shell script required 2 parameters(Updated date Time & Stored file format either Parquet/orc)
createloadorderstbl.hql 	- create Managed orders table in HIVE and load data from which Sqoop imported path
createloadordersparquet.hql	- create external order_partition_parquet table, and select particular datetime from Order table.
createloadordersorc.hql		- create external order_partition_orc table, and select particular datetime from Order table.