package com.linkit

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

object HbaseHandler extends SharedSparkSession {

  //should be passed as parameter or conf file.
  val COLUMN_FAMILY_CF = "cf"
  val DEFAULTROWKEYCOLUMN = "rowkeyid"
  val ZK_HOST = "localhost"
  val ZK_PORT = "2181"
  val HBASE_MASTER_HOST = "localhost"
  val HBASE_MASTER_PORT = "60000"


  def createTableIfNotExist(tableName:String)={
    //get hbase connection
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
    config.set("hbase.master", HBASE_MASTER_HOST + ":"+ HBASE_MASTER_PORT)
    config.setInt("timeout", 120000)
    ConnectionFactory.createConnection(config)
  }

  /**
    * This method shoud be used to prepare  Return a DataFrame with a rowkey column.
    *
    * @param df
    * @param firstColumn
    * @param secColumn
    * @return
    */
  def createRowkeyColumn(df: DataFrame, firstColumn:String, secColumn:String) :DataFrame ={
    df.withColumn(DEFAULTROWKEYCOLUMN, concat(col(firstColumn), lit("|"),col(secColumn)) )
  }

  /**
    * Ingest a DataFrame into a Hbase table.
    *
    * @param df - dataframe
    * @param catalog - catalog specifying the Hbase table structure
    */
  def writeDfHabase( df:DataFrame,  catalog:String) = {
    //put.add(rk, column, value)

    if(hasColumn(df, DEFAULTROWKEYCOLUMN)){
      log.info("*** WRITING INTO HBASE ***")
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

  /**
    * Load tables from Hbase.
    * This method receive a catalog as parameter containing all structured information about the table required
    * example:{
    *          "table":{"namespace":"default", "name":"dangerous_driver"},
    *          "rowkey":"key",
    *          "columns":{
    *          "rowkeyid":{"cf":"rowkey", "col":"key", "type":"string"},
    *          "eventId":{"cf":"${COLUMN_FAMILY_CF}", "col":"eventId", "type":"int"}
    *          }
    *         }
    *The parameter : HBaseTableCatalog.newTable -> "5" should be passed as parameter or conf file. That parameter represent the number of regions server
    * by Hbase table/row
    *
    * @param catalog
    * @return
    */
  def loadHbaseTable(catalog:String):DataFrame ={
    sparkSession.read.options(Map(HBaseTableCatalog.tableCatalog -> catalog,  HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

  }

  /**
    * To resolve an internal Hbase error, the method bellow cast a column to String if needed
    *
    * @param dataFrame
    * @return
    */
  def castTimestampAsString(dataFrame: DataFrame,columnName:String)={
    dataFrame.withColumn(columnName, dataFrame(columnName).cast("String") )
  }

  /**
    * Check if a column exist into a DF
    * @param df
    * @param colName
    * @return
    */
  private def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)


  //This should be into a conf file
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


