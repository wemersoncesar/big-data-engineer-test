package com.linkit

import org.apache.spark.sql.{DataFrame, SaveMode}


class HiveHandler extends SharedSparkSession {

  def saveDF(df:DataFrame, dbname:String, tablename:String, destPath: String): Unit ={

    val formattedDf = removeHyphen(df)
    val fullDestPath = destPath.concat(dbname).concat("/").concat(tablename)
    val fieldsStr = createFieldString(formattedDf)
    log.info("*************************************")
    log.info(s">> ${formattedDf}")
    log.info(s">> ${fullDestPath}")
    log.info(s">> ${fieldsStr}")
    log.info("*************************************")

    //save to HDFS
    formattedDf.write.mode(SaveMode.Append).orc(fullDestPath)

    //creating hive table
    val query = s"CREATE EXTERNAL TABLE IF NOT EXISTS  ${dbname}.${tablename} (${fieldsStr} ) " +
      s"STORED AS ORC  LOCATION '${fullDestPath}'"
     sql(query)
  }

  /**
    * This method removes dash sign(-) from column name that is not supported by Hive
    *
    * @param df
    * @return
    */
  def removeHyphen(df: DataFrame): DataFrame = {
    val columnName = df.columns.toSeq.map(columnName => columnName.replace("-","_"))
    df.toDF(columnName:_*)
  }


  def createFieldString(df: DataFrame):String ={
    var fieldsStr = ""
    df.schema.fields.foreach(f => {
        fieldsStr += s" " +f.name +" " +f.dataType.typeName + ","
      }
    )
    //patch permit us slice a character in text/word: ex: in this case = ...int, ) to ...int )
    fieldsStr.patch(fieldsStr.lastIndexOf(','), "", 1).replace("integer","int")
  }


  def getFullTable(dbName:String, tableName:String): DataFrame ={
    sql(s"select * from $dbName.$tableName")
  }

}
