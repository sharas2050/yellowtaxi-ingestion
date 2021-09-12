package com.taxi.sarunas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSetupObject {
  def createSpark (AppName: String = "DefaultAppName") : SparkSession = {
    val sparkConf: SparkConf = new SparkConf().setAppName(AppName)
      .set("spark.yarn.queue", "default")
      .set("spark.driver.allowMultipleContexts", "true")
    val sparkS: SparkSession = SparkSession.builder().config(sparkConf)
      .enableHiveSupport()
      //.master("local")
      .getOrCreate()
    sparkS
  }
}