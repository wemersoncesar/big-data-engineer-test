package com.linkit

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

class HadoopHandler extends SharedSparkSession {
  //val log = Logger.getLogger(FileName.getClass)
  val hadoopConf = new Configuration()
  val hdfs =  FileSystem.get(new java.net.URI("hdfs://sandbox-hdp.hortonworks.com:8020"), hadoopConf)



  def uploadDataFilesToHiveDir(srcPath:String, destPath:String): Unit = {

    log.info("*** Getting a list of files")
    val filesList = getListOfCSVFiles(new File(srcPath))

    log.info("*** Put files into HDFS")

    filesList.foreach( file => {
      val hdfsPath =  new Path ( destPath+"/" + removeFileExtensions(file.getName))
      try {
        hdfs.copyFromLocalFile( new Path(srcPath +"/"+ file.getName ),hdfsPath)
      }
      catch { case e : Throwable => { e.printStackTrace()} }
    })
  }


  /**
    *
    *
    * @param files
    * @return
    */
  def getDataFramesMap(files: List[File]): Map[String, DataFrame] = {
    files.map(file =>
      (removeFileExtensions(file.getName),
        sparkSession.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv("file:///" + file.getAbsolutePath))).toMap
  }


  /**
    * The method return only csv files as a list from a directory.
    *
    * @param dir
    * @return List[File]
    */
  def getListOfCSVFiles(dir: File): List[File] = {
    if (dir.exists()) dir.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv")).toList
    else List[File]()
  }


  /**
    * Remove the filename extension and special characters.
    *
    * @param file
    * @return => string containing a filename without extension
    */
  def removeFileExtensions(file:String):String = {
    if(file.contains("."))
        file.dropRight(file.length - file.lastIndexOf(".")).replaceAll("[\\\\/:*?\"<>|]", "")
    else file

  }

}
