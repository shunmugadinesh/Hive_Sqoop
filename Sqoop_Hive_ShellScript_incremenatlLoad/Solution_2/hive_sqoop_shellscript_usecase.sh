# Hive Sqoop - Shell scripting
#Read input and assignit to variables
lastvalue=\'$1\'
fileformat=$2
echo -e "last value: $lastvalue , fileformat: $fileformat \n"

tableName=orders_partition_"$fileformat"1;
echo -e "\n Hive external table name: $tableName"

#Function to display result for each step
DisplayResult()
{
step=$1
result=$2
if [ "$result" -eq 0 ] ; then
	echo -e "\n$step: Pass"
else
	echo -e "\n$step: Fail"
	echo -e "\n Exit from shell script"
	exit 1
fi
}

#Sqoop import
sqoop import --connect jdbc:mysql://localhost/custdb --username root --password root --table orders --m 1 --target-dir hiveusecase/orders --incremental lastmodified --append --check-column updts --last-value "$lastvalue"
result=$?
DisplayResult "sqoop import" $result

#Hive - create managed table
hive -e "create table if not exists orders_managed1 (id int,product varchar(100),qty int,amount float,updts timestamp)
row format delimited fields terminated by ',';"
result=$?
DisplayResult "Hive - create managed table" $result

#Hive - load data inpath
hive -e "load data inpath 'hiveusecase/orders' into table orders_managed1;"
result=$?
DisplayResult "Hive - load data inpath" $result

#Hive - create external table based on fileformat
hive -e "create external table if not exists $tableName (id int,product varchar(100),qty int,amount float,updtime string)
partitioned by (upddt date)
row format delimited fields terminated by ','
stored as $fileformat;"
result=$?
DisplayResult "Hive - create external table based on fileformat" $result

#Hive - insert data into external table using upddt partition
hive -e "Insert into table $tableName partition (upddt)
select id,product,qty,amount,split(updts,' ')[1] AS updtime, split(updts,' ')[0] AS upddt
from orders_managed1 
where updts>$lastvalue"
result=$?
DisplayResult "Hive - insert data into external table using upddt partition" $result

