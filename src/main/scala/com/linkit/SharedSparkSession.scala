package com.linkit

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SharedSparkSession extends Serializable {


  val log: Logger = LogManager.getLogger(getClass)

  val isLocal = true

  val warehouseLocation =  "hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse"

  val sparkSession =     if(isLocal ) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      SparkSession.builder()
        .master("local[2]").enableHiveSupport()
        .appName("DataEngTest").config(sparkConfig).getOrCreate()
    }else{
      SparkSession.builder()
        .appName("Linkit Assessment")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .appName("DataEngTest").getOrCreate()
    }




}
