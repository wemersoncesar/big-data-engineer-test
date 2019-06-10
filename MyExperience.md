#My experience and further notes

Put here general information about the steps you took, what you liked, disliked, why did you do X instead of Y and so on.

## HDP Sandbox
I usually to have all service installed on my machine, standalone (integrated with zookeeper, if necessary) 
avoiding to use any Big Data distributions just to save RAM memory, but for this test 
I decided using the HortonWorks Sandbox due to my familiarity with that and I didn't have Hive installed.  

- First issue was found on HDP 3.x sandbox. It could make me waste time, so I changed that to HDP 2.6.5 which I already have a image installed on my Virtualbox.  
- Fist job running: There wasn't resource allocated to default queue on capacity scheduler, so I changed that to 50% / 100% and got able to run the job.
- Few memory and resource on my machine (Macbook pro i7 16GB) was a problem too, so I had to kill some yarn (yarn application -kill app...) 
process to get free resources, since I could not reserving more RAM to VirtualBox.


## HDFS & Hive on Spark

This part of assessment was interesting and really near from real problems that we can face daily. 
<br>
The solution of this part is quite simple and it starts by running the class MainClass and it will:   
<br>    1) The method `uploadDataFilesToHiveDir` gets the local files and put to HDFS located in /linkit/data-spark and add the file names as a path e.g. /linkit/data-spark/drivers/drivers.csv
<br>    2) List files into directory and create a Map of DataFrames.
<br>    3) Rename columns to replace dash sign (-) to underscore (_) 
        symbol (** Hive doesn't support dash in column name). 
 <br>   4) After the DF is prepared, the method `createORCTableForEachCSV` will create a External Table (if not exist) 
 and save the files as ORC into the destination path
 
 After the tables are already created, the method `getTable` will return the Hive table as DataFrame, in this case, drivers and timesheet (separately).
   
<br> The Drivers and Timesheet dataframes are joind and the columns Hours_logged and Miles_logged are summed.

<br> 

    //Join DFs
    val driversTime = drivers.as("dr")
    .join(timesheet.as("ts"), $"dr.driverId" === $"ts.driverId")
    .select($"dr.driverId",$"dr.name", $"ts.hours_logged",$"ts.miles_logged")
    
    //The sum of hours_logged and miles_logged
     driversTime.groupBy( $"dr.driverId",$"dr.name",$"ts.hours_logged",$"ts.miles_logged")
            .agg(sum("ts.hours_logged").as("hours_logged"), sum("ts.miles_logged").as("miles_logged"))
            .show()


 <br>

## HBase

This task asked me a little more effort. In Brazil I had used a library called Hbase-rdd based on Cloudera distribution 
but I could not use it for Hortonworks Sandbox. 
Getting started the task I decided to use Hbase-client because is a native library to connect to Hbase, 
but the insert method and Classes doesn't support DataFrame. So, I decided to use HortonWorks Spark-Hbase Connector.     
<br> Issues:
<br> - I had issue the Spark version related [here](https://github.com/hortonworks-spark/shc/issues/191) when I tried to insert data to Hbase using Spark-Hbase Connector.
<br> - Hard work to harmonizing the Libraries version with Spark/Scala and Habase version. 
I had to change the spark version sometimes to skip of some issues.

###### Commentaries for each question:
<br>
 - Create a table dangerous_driving on HBase 
<br> - I used the native Hbase-client here to have more control of the create table process, e.q: check if it already exist.

 - load dangerous-driver.csv
<br> - For this step I used HortonWorks Spark-Hbase Connector that supports Dataframe to read and write data. 
Native Hbase-client can be boilerplate coding when you need to save a DF into Hbase.   

- add a 4th element to the table from extra-driver.csv
<br> - Nice exercise here! I had already decided to create a new rowkey column before save da data into Hbase composed by two columns. 
For that first version, I choose driverId and eventTime columns to merge into a rowkey column, 
so when I tried to add that only one line with the same eventId into Hbase I had no problem. 
 
- Update id = 4 to display routeName as Los Angeles to Santa Clara instead of Santa Clara to San Diego
<br>There are very important concepts covered about Hbase and Big Data in general here. 
<br>    - We must not change the original data to adapt it to Hbase, incrementing the eventId to 4 would lose the data traceability give us a wrong result if we try to compare with the original data. So, to get that fourth row I used the dirverId and eventId concatenated. 

- Outputs to console the Name of the driver, the type of event and the event Time if the origin or destination is Los Angeles.
<br> Just a simple select on DataFrame, but important change the values to the same uppercase or lowwecase.
``` 
 .select("driverName", "eventType", "eventTime", "eventTime")
      .where(lower(col("routeName")).contains("Los Angeles".toLowerCase))
```

## Spark Structured Streaming

<br> Structured streaming is a modern streaming tool that was released in Spark 2.x version in the end of 2017, 
but It becames a production realease only in Spark 2.2.x which was removed the experimental code. 
There are some distinctions between Spark Streaming and Structured Streaming, the main one is that Structure Streaming 
has no micro batch concept and is more closer to the realtime processing.
Another advantages is different modes of data processing we can use (Complete, Append and Update mode) and Structure Streaming 
uses DataFrame internaly, there are tons of sites and blogs comparations that say DataFrame is more optimized than RDD.




