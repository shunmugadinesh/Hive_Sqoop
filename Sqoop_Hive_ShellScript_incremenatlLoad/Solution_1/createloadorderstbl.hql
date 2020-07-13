--create manage table as we need to overwrite the data always
create table if not exists custdb.orders(id int, product string, qty int, amount float, updts timestamp) row format delimited fields terminated by ',';

--dump sqoop imported data into orders table.
load data inpath '/user/hduser/custdb_orders/' overwrite into table custdb.orders;
