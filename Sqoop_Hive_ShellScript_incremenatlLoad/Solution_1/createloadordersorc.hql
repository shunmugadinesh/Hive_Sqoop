--create external for orc file format
create external table if not exists custdb.orders_partition_orc(id int, product string,qty int, amount float) partitioned by(updts string) stored as orc;

--insert data into orders_partition_orc table
insert into custdb.orders_partition_orc partition(updts='${hiveconf:orc}')
select id,product,qty,amount from custdb.orders where updts = '${hiveconf:orc}';
