package com.linkit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
object StreamingProducer extends SharedSparkSession {

  //should be passed as parameter or conf file
  val linkitwarehousepath = "/linkit/data-spark/"
  val filesPath = "data-hbase/dangerous-driver"
  val kafka_host = "localhost"
  val kafka_port = "9092"
  val topic = "linkit_dangerous_driver"

  def main(args: Array[String]): Unit = {
    //Reading data from a path.
    //
    val csvDF = sparkSession
      .readStream
      .format("csv")
      .option("sep", ",")
      .option("header", true)
      .option("mode", "DROPMALFORMED")
      .schema(userSchema).load(linkitwarehousepath+filesPath)

    //transformations here.
    val prepareDF = csvDF.select(to_json(struct("eventId", "driverId")).alias("value") )

    //writing on Kafka
    val stream = prepareDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_host+":"+kafka_port)
      .option("checkpointLocation", linkitwarehousepath+"/checkpoint")
      .option("topic", topic)
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
