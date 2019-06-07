#My experience and further notes

Put here general information about the steps you took, what you liked, disliked, why did you do X instead of Y and so on.

## HDP Sandbox

I started using the Sandbox 3.x and I got some issues and since was not specified a version I started to use the Sandbox 2.6.5  

- First issue was found on HDP 3.x sandbox. For do not waste time, i change to HDP 2.6.5
- Fist job running: There wasn't resource allocated to default queue on capacity scheduler, I changed it to 50% / 100%
- Few memory and resource on my machine was a problem too, so I had to kill some yarn (yarn application -kill app...) process to get resources.


## HDFS & Hive on Spark

This part of assessment was interesting and really near from real problems we can face daily. 
<br> 
- upload the .csv files on data-spark to HDFS
<br>
- create tables on Hive for each .csv file
<br>
- output a dataframe on Spark that contains DRIVERID, NAME, HOURS_LOGGED, MILES_LOGGED so you can have aggregated information about the driver.
<br>
