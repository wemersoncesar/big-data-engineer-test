package com.linkit

object StreamingReceiver extends SharedSparkSession {

  def main(args: Array[String]): Unit = {

    val receiver = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("subscribe", "test")
      .load()
      .select("value")
      .writeStream
      .format("json")
      .option("path", "linkit")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()
      .awaitTermination()



  }



}
