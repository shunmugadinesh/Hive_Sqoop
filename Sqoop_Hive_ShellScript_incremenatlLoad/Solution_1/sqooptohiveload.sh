#!/bin/bash
#Date 	: 15/02/2019
#Version: 1.0
#Create this shell script to import data from Sqoop to HDFS then Hive tables.
#parameter1 - Update date and time
#parameter2 - Stored as Parquet/orc file format

#script homePath
hdfsPath=/home/hduser/SqoopHiveIntegration/

#logFile
logFile=$hdfsPath/sqooptohiveload.log

#remove if log file already exists
rm -f $logFile

#check the script has 2 parameter passed during execution
if [ $# -ne 2 ]
then
	date +"%m-%d-%y %T" >> $logFile
	echo "$0 is required 2 parameter(update timestamp & Parquet or ORC file format) to proceed..." >> $logFile
	exit 1;
fi

#step3 import data from Sqoop to HDFS
sqoop import --connect jdbc:mysql://localhost/custdb --username root --password root --table orders -m 1 --target-dir custdb_orders --incremental lastmodified --check-column updts --last-value "$1" --merge-key id

#check the above execution is success or not
exe_sts=$? 
echo "Sqoop import execution status is $exe_sts" >> $logFile

if [ $exe_sts -eq 0 ]
then
	date +"%m-%d-%y %T" >> $logFile
	echo "Sqoop import is successfully completed..." >> $logFile
	
	#step4 Create and load order table into hive table using hiveQL script
	hive -f $hdfsPath/createloadorderstbl.hql
	
	#Check status of above table creation step
	exe_sts=$?
	if [ $exe_sts -eq 0 ]
	then
		date +"%m-%d-%y %T" >> $logFile
		echo "Hive orders table created & loaded successfully completed..." >> $logFile
	else
		date +"%m-%d- %T" >> $logFile
		echo "Hive orders table creation FAILED..." >> $logFile
		exit 3;
	fi

	#step5 Check File format
	if [ $2 == "parquet" ]
	then
		#call parquet create and load hQL file.
		hive -f $hdfsPath/createloadordersparquet.hql --hiveconf parq="$1"
		
		#Check status of above parquet table creation step
		exe_sts=$?
		if [ $exe_sts -eq 0 ]
		then
			date +"%m-%d-%y %T" >> $logFile
                        echo "Hive external table orders_partition_parquet create & load successfully completed..." >> $logFile
		else
			date +"%m-%d-%y %T" >> $logFile
                        echo "Hive external table orders_partition_parquet create & load FAILED..." >> $logFile
			exit 4;
		fi
	elif [ $2 == "orc" ]
	then
		#call orc create and load hQL file.
                hive -f $hdfsPath/createloadordersorc.hql --hiveconf orc="$1"

		#Check status of above ORC table creation step
                exe_sts=$?
                if [ $exe_sts -eq 0 ]
                then
                       date +"%m-%d-%y %T" >> $logFile
                       echo "Hive external table orders_partition_orc create & load successfully completed..." >> $logFile
                else
                       date +"%m-%d-%y %T" >> $logFile
                       echo "Hive external table orders_partition_orc create & load FAILED..." >> $logFile
                       exit 5;
                fi
	fi #end of file format check(Parquet or ORC)

else
	date +"%m-%d-%y %T" >> $logFile
	echo "Sqoop import is FAILED..." >> $logFile
	exit 2;
fi #end of sqoop import status

