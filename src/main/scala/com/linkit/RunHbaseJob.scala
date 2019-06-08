package com.linkit

import com.linkit.HbaseHandler._
import org.apache.spark.sql.functions.{col, lit, lower, when}


/*
* create a table dangerous_driving on HBase
* load dangerous-driver.csv
* add a 4th element to the table from extra-driver.csv
* Update id = 4 to display routeName as Los Angeles to Santa Clara instead of Santa Clara to San Diego
* Outputs to console the Name of the driver, the type of event and the event Time if the origin or destination is Los Angeles.
* */




object RunHbaseJob {
  def main(args: Array[String]): Unit = {
    //create Hbase table
    createTableIfNotExist("dangerous_driver")


    val dbname = "linkitdb"
    val tableName = "dangerous_driver"
    val sourceFile = "files/data-hbase/extra-driver/extra-driver.csv"
    val driverId = "78"
    val eventId = "1"


    val hive = new HiveHandler



    //val df = hive.getFullTable(dbname,"drivers")

    //reading data.
    val dd = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep","," ).csv(sourceFile)

    //Changing timestamp type to String
    val ddCastTm = castTimestampAsString(dd,"eventTime" )

    //creating a rowkey column concatenating two columns (could be more) to ensure that each rows will be inserted.
    val ddRowKey = createRowkeyColumn(ddCastTm,"driverId", "eventId")

    //Writing into Hbase
    writeDfHabase( ddRowKey, dangerousDriverCatalog)

    // Getting data from Hbase
    val dangDriversDF = loadHbaseTable(dangerousDriverCatalog)

    dangDriversDF.show()


    //Loading extra-Driver
    val extraDriver = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep","," ).csv(sourceFile)

    //Changing timestamp type to String
    val extraDriverDF = castTimestampAsString(extraDriver,"eventTime" )

    //creating a rowkey concatenating driverId and eventId
    val extraDriverRowkey = createRowkeyColumn(extraDriverDF,"driverId", "eventId" )

    log.info(" *** extraDriver  ***")
    //writing extra driver into Habase table
    writeDfHabase(extraDriverRowkey, dangerousDriverCatalog)

    //loading from hbase
    val dgsdriversLoaded = loadHbaseTable(dangerousDriverCatalog).where( col("rowkeyid")  === driverId+"|"+eventId)
    val newdriver = dgsdriversLoaded.withColumn("routeName", when (lower(col("routeName"))
      .equalTo("Santa Clara to San Diego".toLowerCase),  lit ("Los Angeles to Santa Clara")))
    writeDfHabase(newdriver,dangerousDriverCatalog)
    //dgsdriversLoaded.select("rowkeyid", "eventId", "driverId", "eventTime").show()

    //Outputs to console the Name of the driver, the type of event and the event Time if the origin or destination is Los Angeles.
    val drivers = loadHbaseTable(dangerousDriverCatalog)
      .select("driverName", "eventType", "eventTime", "eventTime")
      .where(lower(col("routeName")).contains("Los Angeles".toLowerCase))
    drivers.show()
  }

}
