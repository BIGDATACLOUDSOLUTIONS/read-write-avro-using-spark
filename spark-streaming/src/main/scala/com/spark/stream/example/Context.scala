package com.spark.stream.example

import org.apache.log4j.Level._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.{SQLContext, SparkSession}

object Context {

  implicit lazy val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
  implicit lazy val sc: SparkContext = spark.sparkContext
  implicit lazy val sqlContext: SQLContext = spark.sqlContext
  implicit lazy val metaStore: ExternalCatalog = spark.sharedState.externalCatalog

  implicit lazy val logger: Logger = LogManager.getRootLogger
  spark.conf.set("hive.exec.dynamic.partition", "true")
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.sparkContext.setCheckpointDir("/tmp/" + sc.sparkUser + "_spark/checkpoints")

  def setLogLevel(logLevelString: String = "ERROR"): Unit = {
    logger.info("Setting log level to: " + logLevelString)
    val logLevel = logLevelString.toUpperCase match {
      case "OFF" => OFF
      case "FATAL" => FATAL
      case "ERROR" => ERROR
      case "WARN" => WARN
      case "INFO" => INFO
      case "DEBUG" => DEBUG
      case "TRACE" => TRACE
      case "ALL" => ALL
      case _ => throw new Exception(
        s"""Found loggerLevel= $logLevelString
            Expected value (OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL)"""
      )
    }
    logger.setLevel(logLevel)
  }

}
