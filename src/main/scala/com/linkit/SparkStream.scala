package com.linkit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
object SparkStream extends SharedSparkSession {

  def main(args: Array[String]): Unit = {

    val csvDF = sparkSession
      .readStream
      .format("csv")
      .option("sep", ",")
      .option("header", true)
      .option("mode", "DROPMALFORMED")
      .schema(userSchema).load("files/data-hbase/dangerous-driver")

    //possibility to have transformations here.
    val prepareDF = csvDF.select(to_json(struct("eventId", "driverId")).alias("value") )

    val stream = prepareDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("checkpointLocation", "files/data-hbase/dangerous-driver")
      .option("topic", "test")
      .outputMode(OutputMode.Append())
      .start()

    stream.awaitTermination()
  }

  val userSchema = new StructType()
    .add("eventId", "string")
    .add("driverId", "string")
    .add("driverName", "string")
    .add("eventTime", "string")
    .add("eventType", "string")
    .add("latitudeColumn", "string")
    .add("longitudeColumn", "string")
    .add("routeId", "string")
    .add("routeName", "string")
    .add("truckId", "string")


}
