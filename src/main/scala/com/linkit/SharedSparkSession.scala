package com.linkit

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SharedSparkSession extends Serializable {


  val log: Logger = LogManager.getLogger(getClass)

  val isLocal = true

  //when Hive Support is enable on SparkSession it makes possible query directly.
  val warehouseLocation =  "hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse"

  val sparkSession =     if(isLocal ) {
      val sparkConfig = new SparkConf()
    //Avoid compressing when run data shuffle on local machine:
    //spark.broadcast.compress => Whether to compress broadcast variables before sending them. Generally a good idea.
    //spark.shuffle.compress => Whether to compress map output files. Generally a good idea. Compression will use spark.io.compression.codec.
    //spark.shuffle.spill.compress => Whether to compress data spilled during shuffles. Compression will use spark.io.compression.codec

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
