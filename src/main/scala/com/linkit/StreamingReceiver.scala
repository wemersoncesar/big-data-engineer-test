package com.linkit

import org.apache.spark.sql.DataFrameWriter

object StreamingReceiver extends SharedSparkSession {

  //Should be passed as parameter or conf file
  val kafka_host = "localhost"
  val kafka_port = "9092"
  val topic = "linkit_dangerous_driver"
  val pathDest = "data-hbase/dangerous-driver"
  val linkitWarehousePath = "/linkit/data-spark/"

  def main(args: Array[String]): Unit = {

    //Reading files from kafka topic
    val read = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_host+":"+kafka_port)
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)")

    val write = read.writeStream
      //could be csv, parquet, ORC
        .format("json")
      //Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
      // This is applicable only on the queries where existing rows in the Result Table are not expected to change.
        .outputMode("append")
        .option("failOnDataLoss", "false")
        .option("path", linkitWarehousePath+pathDest)
        .option("checkpointLocation", linkitWarehousePath+"checkpoint")
        .start()

    write.awaitTermination()

  }



}
