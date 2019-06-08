#My experience and further notes

Put here general information about the steps you took, what you liked, disliked, why did you do X instead of Y and so on.

## HDP Sandbox
I usually to have all service installed on my machine, standalone (integrated with zookeeper, if necessary) 
avoiding to use any Big Data distributions just to save RAM memory, but for this test 
I decided using the HortonWorks Sandbox due to my familiarity with that and I didn't have Hive installed.  

- First issue was found on HDP 3.x sandbox. For that does not make me waste time, I changed that to HDP 2.6.5
- Fist job running: There wasn't resource allocated to default queue on capacity scheduler, I changed it to 50% / 100%
- Few memory and resource on my machine was a problem too, so I had to kill some yarn (yarn application -kill app...) process to get free resources.


## HDFS & Hive on Spark

This part of assessment was interesting and really near from real problems that we can face daily. 
<br>
The solution of this part is quite simple and have few steps:   
<br>    1) Upload the file directory to HDFS located in /tmp
<br>    2) List files into directory and create a Map of DataFrames
<br>    3) Rename columns to replace dash sign (-) to underscore (_) 
        symbol (** Hive doesn't support dash in column name). 
 <br>   4) After the DF is prepared, the method createORCTableForEachCSV will create a External Table (if not exist) 
 and save the as ORC file into the destination path
 
 After the tables are already created,  the method getTable will return the DataFrame 
 required by parameter, in this case, drivers and timesheet.
   
<br> The Drivers and Timesheet dataframes are joind and the columns Hours_logged and Miles_logged are summed, 
 

