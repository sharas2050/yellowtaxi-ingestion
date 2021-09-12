CREATE EXTERNAL TABLE IF NOT EXISTS yellowtaxi.tripdata (
  tpep_pickup_datetime VARCHAR(20),
  tpep_dropoff_datetime VARCHAR(20),
  trip_time_m BIGINT,
  passenger_count VARCHAR(20),
  trip_distance VARCHAR(20),
  RatecodeID VARCHAR(20),
  store_and_fwd_flag VARCHAR(20),
  PULocationID VARCHAR(20),
  DOLocationID VARCHAR(20),
  payment_type VARCHAR(20),
  fare_amount VARCHAR(20),
  extra VARCHAR(20),
  mta_tax VARCHAR(20),
  tip_amount VARCHAR(20),
  tolls_amount VARCHAR(20),
  improvement_surcharge VARCHAR(20),
  total_amount VARCHAR(20),
  congestion_surcharge VARCHAR(20),
  load_ts TIMESTAMP
) PARTITIONED BY (
  VendorID VARCHAR(10)
)
STORED AS ORC
LOCATION '/user/hive/yellowtaxi';


		avrodf.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_time_m")
		.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table", tempTable).save()
		
		
val df = spark.sql("select tpep_pickup_datetime, tpep_dropoff_datetime, (bigint(to_timestamp(tpep_dropoff_datetime)) - bigint(to_timestamp(tpep_pickup_datetime))) /60 as trip_time_m, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, load_ts, VendorID from lenta ")


df.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").mode("overwrite").option("table", "tripdata").save()
		
		
		sql("SELECT ws_sold_time_sk, ws_ship_date_sk FROM web_sales WHERE ws_sold_time_sk > 80000)
.write.format(HIVE_WAREHOUSE_CONNECTOR)
.mode("append")
.option("table", "newTable")
.save()