package com.linkit

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

object HbaseHandler extends SharedSparkSession {

  /*
  * create a table dangerous_driving on HBase
  * load dangerous-driver.csv
  * add a 4th element to the table from extra-driver.csv
  * Update id = 4 to display routeName as Los Angeles to Santa Clara instead of Santa Clara to San Diego
  * Outputs to console the Name of the driver, the type of event and the event Time if the origin or destination is Los Angeles.
  * */
  val COLUMN_FAMILY_CF = "cf"
  val DEFAULTROWKEYCOLUMN = "rowkeyid"

  def main(args: Array[String]): Unit = {
    //Upload files to HDFS

    //Approach 1 - preserve historical data
    createTableIfNotExist("dangerous_driver")


    val dbname = "linkitdb"
    val tableName = "dangerous_driver"
    val hive = new HiveHandler

    val driverId = "78"
    val eventId = "1"

    //val df = hive.getFullTable(dbname,"drivers")

    //reading data.
    val dd = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep","," ).csv("files/data-hbase/dangerous-driver/dangerous-driver.csv")

    //Changing timestamp type to String
    val ddCastTm = castTimestampAsString(dd,"eventTime" )

    //creating a rowkey column concatenating two columns (could be more) to ensure that each rows will be inserted.
    val ddRowKey = createRowkeyColumn(ddCastTm,"driverId", "eventId")

    //Writing into Hbase
    writeDfHabase( ddRowKey, dangerousDriverCatalog)

    // Testing getting data from Hbase
    val dangDriversDF = loadHbaseTable(dangerousDriverCatalog)

    dangDriversDF.show()


  //Loading extra-Driver
    val extraDriver = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep","," ).csv("files/data-hbase/extra-driver/extra-driver.csv")

    //Changing timestamp type to String
    val extraDriverDF = castTimestampAsString(extraDriver,"eventTime" )
    //creating a rowkey
    val extraDriverRowkey = createRowkeyColumn(extraDriverDF,"driverId", "eventId" )
    log.info(" extraDriverRowkey ")
    extraDriverRowkey.show()
    writeDfHabase(extraDriverRowkey, dangerousDriverCatalog)



    /*
        Update id = 4 to display routeName as Los Angeles to Santa Clara instead of Santa Clara to San Diego

     */


    //Update row
    //loading table
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



  def createTableIfNotExist(tableName:String)={
    val conn = getConnection()
    val admin = conn.getAdmin

    val table = new HTableDescriptor(TableName.valueOf(tableName))
    table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_CF).setCompressionType(Algorithm.NONE))

    if (admin.tableExists(table.getTableName))
      log.warn("The Table [" + table.getTableName.getNameAsString + "] is already existed.")
    else {
      try{
        println("Creating new table... ")
        admin.createTable(table)
        println("Done.")
      }catch {
        case e : Exception => e.printStackTrace()
      }finally {
        conn.close()
      }

    }

  }

  def  getConnection(): Connection = {
    val config = HBaseConfiguration.create()
    config.set("hbase.master", "localhost:60000")
    config.set("hbase.zookeeper.quorum", "localhost")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.setInt("timeout", 120000)
    ConnectionFactory.createConnection(config)
  }


  def createRowkeyColumn(df: DataFrame, firstColumn:String, secColumn:String) :DataFrame ={
    df.withColumn("rowkeyid", concat(col(firstColumn), lit("|"),col(secColumn)) )
  }


  def writeDfHabase( df:DataFrame,  catalog:String) = {
    //put.add(rk, column, value)

    if(hasColumn(df, DEFAULTROWKEYCOLUMN)){
      println("*** WRITING INTO HBASE ***")
      df.write
        .options(Map(HBaseTableCatalog.tableCatalog -> catalog,  HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }else{
      log.info("****************************")
      log.warn("[ERROR]: table without rowkey ")
      log.info("****************************")
      //throw an exception
    }

  }


  def loadHbaseTable(catalog:String):DataFrame ={
    sparkSession.read.options(Map(HBaseTableCatalog.tableCatalog -> catalog,  HBaseTableCatalog.newTable -> "5"))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  }




  def getMaxId (dataFrame: DataFrame, id:String):Int={
    var maxId = 0
    dataFrame.agg(max(id)).foreach(row => { maxId =  row.getInt(0) })
    maxId
  }

  /**
    * Since Hbase doesn't support timestamp, the method bellow cast Timestamp to String
    *
    * @param dataFrame
    * @return
    */
  def castTimestampAsString(dataFrame: DataFrame,columnName:String)={
    dataFrame.withColumn(columnName, dataFrame(columnName).cast("String") )
  }

  def appendHbaseTable(df: DataFrame, catalog:String) ={
    val dfLoaded = loadHbaseTable(catalog)
    // dfLoaded.agg(max("id")).select("id")
    df.withColumn("id", monotonically_increasing_id()).show()

    dfLoaded.union(df).show()

  }


  private def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)


  def dangerousDriverCatalog = s"""{
                   |"table":{"namespace":"default", "name":"dangerous_driver"},
                   |"rowkey":"key",
                   |"columns":{
                   |"rowkeyid":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"eventId":{"cf":"${COLUMN_FAMILY_CF}", "col":"eventId", "type":"int"},
                   |"driverId":{"cf":"${COLUMN_FAMILY_CF}", "col":"driverId", "type":"int"},
                   |"driverName":{"cf":"${COLUMN_FAMILY_CF}", "col":"driverName", "type":"string"},
                   |"eventTime":{"cf":"${COLUMN_FAMILY_CF}", "col":"eventTime", "type":"string"},
                   |"eventType":{"cf":"${COLUMN_FAMILY_CF}", "col":"eventType", "type":"string"},
                   |"latitudeColumn":{"cf":"${COLUMN_FAMILY_CF}", "col":"latitudeColumn", "type":"double"},
                   |"longitudeColumn":{"cf":"${COLUMN_FAMILY_CF}", "col":"latitudeColumn", "type":"double"},
                   |"routeId":{"cf":"${COLUMN_FAMILY_CF}", "col":"routeId", "type":"int"},
                   |"routeName":{"cf":"${COLUMN_FAMILY_CF}", "col":"routeName", "type":"string"},
                   |"truckId":{"cf":"${COLUMN_FAMILY_CF}", "col":"truckId", "type":"int"}
                   |}
                   |}""".stripMargin
}


