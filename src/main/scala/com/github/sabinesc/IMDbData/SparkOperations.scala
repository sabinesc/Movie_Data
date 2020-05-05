package com.github.sabinesc.IMDbData

import java.util.Properties
import java.sql.{DriverManager, Connection}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOperations {

  def init(): SparkSession = {
    val spark = SparkSession.builder.appName("IMDb_Data").config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def dataFrameFromTsvFile(filePath: String): DataFrame = {
    init().read.format("csv").option("sep", "\t").option("header", true).load(filePath).toDF()
  }

  def createDbTable(dbPath:String,tableName:String,  data:DataFrame, username:String, password:String):Unit = {
    init()
    println("Connecting to database...")
    val connectionProperties = new Properties()
    if(username !="" && password !="") {
      connectionProperties.put("user", username)
      connectionProperties.put("password", password)
    }
    val connection:Connection = DriverManager.getConnection(dbPath)
    println("Table is being uploaded...")
    data.write.jdbc(dbPath, tableName, connectionProperties)
    connection.close()
    println("Table successfully uploaded")
  }
}
