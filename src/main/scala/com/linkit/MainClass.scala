package com.linkit

import java.io.File

import org.apache.spark.sql.functions.sum

object MainClass extends SharedSparkSession {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._

    val dest = "hdfs://sandbox-hdp.hortonworks.com:8020/linkit/data-spark/"
    val src = "/tmp/data-spark/"
    val dbname = "linkitdb"

    val hadoopHandle = new HadoopHandler()
    val hiveHandle= new HiveHandle()
    hadoopHandle.uploadDataFilesToHiveDir(src, dest)

    val fileList = hadoopHandle.getListOfCSVFiles(new File(src))
    val dfList = hadoopHandle.getDataFramesMap(fileList)

    //create hive table for each .csv file

    dfList.foreach( dfmap => {
      //create hive table with
      hiveHandle.saveDF(dfmap._2, dbname, dfmap._1, dest)
    })


    //Instruction:
    //output a dataframe on Spark that contains
    // DRIVERID, NAME, HOURS_LOGGED, MILES_LOGGED so you can have aggregated information about the driver.

    val drivers = hiveHandle.getFullTable(dbname,"drivers")
    drivers.show()

    val timesheet = hiveHandle.getFullTable(dbname, "timesheet")
    timesheet.show()

    val driversTime = drivers.as("dr").join(timesheet.as("ts"), $"dr.driverId" === $"ts.driverId")
      .select($"dr.driverId",$"dr.name",$"ts.hours_logged",$"ts.miles_logged")

    driversTime.show()

    //The amount of logged hours and logged miles per user
    driversTime.groupBy( $"dr.driverId",$"dr.name",$"ts.hours_logged",$"ts.miles_logged")
        .agg(sum("ts.hours_logged").as("hours_logged"), sum("ts.miles_logged").as("miles_logged")  )
      .show()



  }



}
