--create external for parquet file format
create external table if not exists custdb.orders_partition_parquet(id int, product string,qty int, amount float) partitioned by(updts string) stored as parquet;

--insert data into orders_partition_parquet table
insert into custdb.orders_partition_parquet partition(updts='${hiveconf:parq}')
select id,product,qty,amount from custdb.orders where updts = '${hiveconf:parq}';
