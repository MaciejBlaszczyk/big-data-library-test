package com.virtuslab.io.spark.writer

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import java.nio.file.{Path, Paths}
import java.util.UUID

trait SparkContext {
  private val temporaryDirectory: Path = Paths.get(s"/tmp/SparkTests-${UUID.randomUUID().toString}").toAbsolutePath
  private val warehouseDirectory: Path = Paths.get(temporaryDirectory + "/warehouse_dir").toAbsolutePath
  private val hive_scratch_dir: Path = Paths.get(temporaryDirectory + "/hive_scratch_dir").toAbsolutePath
  temporaryDirectory.toFile.deleteOnExit()

  def partitionsCount: Short = 15

  implicit lazy val spark: SparkSession = init()

  def init(): SparkSession = {
    class FileCleanupListener extends SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        FileUtils.deleteDirectory(temporaryDirectory.toFile)
      }
    }

    val ss = SparkSession
      .builder()
      .appName("Spark test")
      .master("local[2]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.sql.shuffle.partitions", partitionsCount.toString)
      .config("spark.executor.instances", "2")
      .config("spark.executor.memory", "2g")
      .config("spark.local.dir", temporaryDirectory.toString)
      .config("hive.metastore.warehouse.dir", warehouseDirectory.toString)
      .config("spark.sql.warehouse.dir", warehouseDirectory.toString)
      .config("hive.exec.scratchdir", hive_scratch_dir.toString)
      .enableHiveSupport()
      .getOrCreate()

    ss.sparkContext.addSparkListener(new FileCleanupListener)
    ss
  }

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  spark.sql("select 1").collect()
  spark.sql("select 1").count
}
