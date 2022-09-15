package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.FileManager.listDirectories
import com.virtuslab.io.spark.writer.TableMetadataManager._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.{FlatSpec, Matchers}

final case class SomeDto(number: Int, letter: String, partition_number: Int, partition_letter: String)

class TableMetadataManagerTests extends FlatSpec with SparkContext with Matchers {
  import spark.implicits._

  implicit val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val ds = Seq(SomeDto(1, "a", 1, "a"), SomeDto(2, "b", 2, "b")).toDS()

  it should "create table in metastore" in {
    val table = TableIdentifier("example_table_1", Some("default"))
    val location = extractTableLocation(table)

    createEmptyExternalTable(ds.toDF, Seq("partition_number", "partition_letter"), table, location)

    spark.sessionState.catalog.tableExists(table) shouldBe true
  }

  it should "get location of a table that exists" in {
    val table = TableIdentifier("example_table_3", Some("default"))
    val location = extractTableLocation(table) + "test"
    createEmptyExternalTable(ds.toDF, Seq("partition_number", "partition_letter"), table, location)

    extractTableLocation(table) shouldBe location
  }

  it should "add partitions to table" in {
    val table = TableIdentifier("example_table_4", Some("default"))
    val location = extractTableLocation(table)
    val partitionsSpecs = Seq(
      Map("partition_number" -> "1", "partition_letter" -> "a"),
      Map("partition_number" -> "2", "partition_letter" -> "b")
    )
    createEmptyExternalTable(ds.toDF, Seq("partition_number", "partition_letter"), table, location)

    addPartitions(table, partitionsSpecs)

    spark.sessionState.catalog.listPartitions(table).map(_.spec) should contain theSameElementsAs partitionsSpecs
  }

  it should "get partitions cols in order from a table" in {
    val table = TableIdentifier("example_table_5", Some("default"))
    val location = extractTableLocation(table)
    val orderedPartitionsCols = Seq("partition_number", "partition_letter")
    ds.write
      .mode(SaveMode.Overwrite)
      .partitionBy(orderedPartitionsCols: _*)
      .option("path", location)
      .saveAsTable(table.toString())

    getPartitionColsInOrder(table) should contain theSameElementsInOrderAs orderedPartitionsCols
  }

  it should "drop one partition out of two existing and delete its files" in {
    val table = TableIdentifier("example_table_6", Some("default"))
    val location = extractTableLocation(table)
    val orderedPartitionsCols = Seq("partition_number", "partition_letter")
    val partitionsSpecs = Seq(
      Map("partition_number" -> "1", "partition_letter" -> "a"),
      Map("partition_number" -> "2", "partition_letter" -> "b")
    )
    ds.write
      .mode(SaveMode.Overwrite)
      .partitionBy(orderedPartitionsCols: _*)
      .option("path", location)
      .saveAsTable(table.toString())

    dropPartitions(table, partitionsSpecs.take(1))

    spark.sessionState.catalog.listPartitions(table).map(_.spec) shouldBe partitionsSpecs.tail
    listDirectories(location) should contain theSameElementsAs Seq(s"$location/partition_number=2")
  }
}
