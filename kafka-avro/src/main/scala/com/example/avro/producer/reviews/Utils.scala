package com.example.avro.producer.reviews

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{FileSystems, Files, Paths}
import scala.reflect.io.Directory

object Utils {

  val moduleRootDir: String = FileSystems.getDefault.getPath("").toAbsolutePath.toString + "/kafka-avro/"
  implicit lazy val conf: Config = ConfigFactory.parseFile(new File(s"$moduleRootDir/src/main/resources/config/review_producer.conf"))

  def deleteNonEmptyDir(directory: String): Unit = {
    val dir = new Directory(new File(directory))
    dir.deleteRecursively()
  }

  def deleteFileIfExists(filePath: String): Unit = {
    Files.deleteIfExists(Paths.get(filePath))
  }

  def createDirIfNotExists(outputDir: String):Unit={
    val path = Paths.get(outputDir)
    Files.createDirectories(path)
  }

  def recreateOutputDir(outputDir: String): Unit = {
    val path = Paths.get(outputDir)
    if (Files.exists(path)) deleteNonEmptyDir(outputDir)
    Files.createDirectories(path)
  }
}
