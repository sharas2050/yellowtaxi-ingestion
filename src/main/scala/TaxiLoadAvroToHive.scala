package com.taxi.sarunas {

  import org.apache.log4j.LogManager
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{SparkSession}
  import org.apache.spark.SparkContext

  object TaxiLoadAvroToHive extends App {

    var argsArr = new Array[String](3)
    val logger = LogManager.getLogger("TaxiLoadAvroToHive")
    logger.info("Loading avro to hive of yellow taxi data initiated")

    try {
      argsArr = args
      if (argsArr.length > 0) {
        logger.info("arguments exist")
        for (arg <- args) {
          logger.info("printing arguments: " + arg.toString())
        }
      }
      else {
        logger.error("No any additional arguments, these are mandatory. Please read instructions before running")
        sys.exit(1)
      }
    } catch {
      case e: Exception => {
        logger.error("error on reading argurments")
        logger.error(e.printStackTrace())
      }
    }

    val spark: SparkSession = SparkSetupObject.createSpark("yellow-taxi-ingestion")
    val spContext: SparkContext = spark.sparkContext
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")

    logger.info("Spark session initiated correctly")

    import spark.implicits._

    if (!args.isEmpty && args.length >= 3) {
      try {

        val csvFileFullPath = args(0) //"/user/maria_dev/yellow_tripdata_2020-01.csv"
        val hiveDbName = args(1) //"yellowtaxi"
        val hiveTableName = args(2) //"tripdata"
        val overwriteHiveData = if (args(3) != null && !args(3).isEmpty() && args(3) == "overwrite") "Yes" else "No"

        val load_ts = current_timestamp()

        val sourceCsv = spark.read.format("csv")
          .option("delimiter",",").option("quote","").option("header","true")
          .load(csvFileFullPath)

        sourceCsv.write.mode("overwrite")
          .format("com.databricks.spark.avro")
          .save(csvFileFullPath.dropRight(4))

        val avrodf = spark.read.format("com.databricks.spark.avro").load(csvFileFullPath.dropRight(4))
          .withColumn("load_ts", lit(load_ts))
        logger.info("Reading avro file is done")


        avrodf.createOrReplaceTempView("lenta")
        logger.info("creating spark temp view from avro data is done")

        val insertsql = "select tpep_pickup_datetime, tpep_dropoff_datetime, " +
          "(bigint(to_timestamp(tpep_dropoff_datetime)) - bigint(to_timestamp(tpep_pickup_datetime))) /60 as trip_time_m, " +
          "passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, " +
          "payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, " +
          "congestion_surcharge, load_ts, VendorID from lenta"

          if (overwriteHiveData == "Yes") {
            logger.info(s"Overwriting Hive table $hiveDbName.$hiveTableName PARTITION(VendorID)")
            spark.sql(s"insert overwrite table $hiveDbName.$hiveTableName partition (VendorID) $insertsql")
          } else {
            logger.info(s"Appending Hive table $hiveDbName.$hiveTableName PARTITION(VendorID)")
            spark.sql(s"insert into $hiveDbName.$hiveTableName partition (VendorID) $insertsql")
          }

        logger.info("Hive table insert on PARTITION(VendorID) from temp view is done")

      }
      catch {
        case e: Exception =>
          logger.error("Error creating DataFrame and writing to Hive, try again later")
          logger.error(e.printStackTrace())
          sys.exit(1)
      }
    }
    else {
      logger.error("required arguments are empty")
      sys.exit(1)
    }

  }

}
