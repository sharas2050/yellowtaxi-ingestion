# yellowtaxi-ingestion
Application takes csv file from hdfs and converts it to AVRO
AVRO file later is ingested to hive external table

Application takes 4 parameters:

CSV file location in HDFS  
hive database  
hive table  
overwrite table or not

Execute example:
spark-submit --jars /home/maria_dev/spark-avro_2.11-4.0.0.jar --master yarn --deploy-mode client --driver-java-options -Dlog4j.configuration=file:/home/maria_dev/log4j-taxi-ingestion.properties --class com.taxi.sarunas.TaxiLoadAvroToHive /home/maria_dev/taxi-ingestion-0.2.jar /user/maria_dev/yellow_tripdata_sample.csv yellowtaxi tripdata overwrite
