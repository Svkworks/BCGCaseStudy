package com.bcg.Azure

import org.apache.log4j.Level._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/*
#1. Create Azure Blob Storage Account and save STORAGE_NAME,SECRET_ACCESS_KEY somewhere
#2. Add hadoop-azure and azure-storage jars to your /spark/jars/. folder
#3. If it is pyspark add jetty-util jar to /spark/jars/. folder
#4. Add Storage_name and Access_key details in core-site.xml in hadoop/etc/ folder (optional)
#5. Follow the below steps to accecss the files
*/


object Azure_Spark_connectivity extends App {

  Logger.getLogger("org").setLevel(OFF)
  Logger.getLogger("akka").setLevel(OFF)

  var SECRET_ACCESS_KEY = Azure_Credentials.SECRET_ACCESS_KEY
  var SECRET_ACCESS_KEY2 = Azure_Credentials.SECRET_ACCESS_KEY2
  var STORAGE_NAME = Azure_Credentials.STORAGE_NAME

  var CONTAINER = "inputstorage1"
  var FILE_NAME = "movies.csv"
  var fs_acc_key = "fs.azure.account.key." + STORAGE_NAME + ".blob.core.windows.net"


  var spark= SparkSession.builder().
    appName("Azure_spark_connectivity").master("local[*]")
    .getOrCreate()

  spark.conf.set(fs_acc_key, SECRET_ACCESS_KEY2)
  spark.conf.set(fs_acc_key, SECRET_ACCESS_KEY)

 var path = s"wasbs://${CONTAINER}@${Azure_Credentials.STORAGE_NAME}.blob.core.windows.net/"

  var df = spark.read.csv(path+"movies.csv")

  df.show(20,true)

}
