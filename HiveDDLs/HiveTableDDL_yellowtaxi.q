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
