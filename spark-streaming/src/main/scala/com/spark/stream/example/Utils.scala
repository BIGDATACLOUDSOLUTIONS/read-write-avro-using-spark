package com.spark.stream.example

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.nio.file.FileSystems

object Utils {

  val moduleRootDir: String = FileSystems.getDefault.getPath("").toAbsolutePath.toString + "/spark-streaming/"



}
