package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.FileFormat.FileFormat
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.net.URI

object TableMetadataManager {

  def createEmptyExternalTable(
    dataFrame: DataFrame,
    partitions: Seq[String],
    table: TableIdentifier,
    tableLocation: String,
    fileFormat: FileFormat = FileFormat.Parquet
  ): Unit = {
    dataFrame
      .limit(0)
      .write
      .option("path", tableLocation)
      .partitionBy(partitions: _*)
      .mode(SaveMode.Overwrite)
      .format(fileFormat.toString)
      .saveAsTable(table.toString())
  }

  def getPartitionColsInOrder(table: TableIdentifier, defaultPartitionCols: Seq[String] = Seq.empty[String])(implicit
    spark: SparkSession
  ): Seq[String] = {
    val catalog = spark.sessionState.catalog
    if (catalog.tableExists(table))
      catalog
        .listPartitionNames(table)
        .headOption
        .map(_.split("/").map(_.takeWhile(_ != '=')).toSeq)
        .getOrElse(defaultPartitionCols)
    else
      defaultPartitionCols
  }

  def tableExists(table: TableIdentifier)(implicit spark: SparkSession): Boolean = {
    spark.sessionState.catalog.tableExists(table)
  }

  def extractTableLocation(table: TableIdentifier)(implicit spark: SparkSession): String = {
    val catalog = spark.sessionState.catalog
    if (tableExists(table))
      catalog.getTableMetadata(table).location.toString
    else
      s"${catalog.getDatabaseMetadata(table.database.getOrElse("")).locationUri.toString}/${table.table}"
  }

  def dropPartitions(table: TableIdentifier, partitionsSpecs: Seq[TablePartitionSpec])(implicit
    spark: SparkSession
  ): Unit = {
    val sparkCatalog = spark.sessionState.catalog
    val oldTable = sparkCatalog.getTableMetadata(table)
    sparkCatalog.alterTable(oldTable.copy(tableType = CatalogTableType.MANAGED))
    // purge = false to move files to trash, retainData = false to delete files
    sparkCatalog.dropPartitions(table, partitionsSpecs, ignoreIfNotExists = true, purge = false, retainData = false)
    sparkCatalog.alterTable(oldTable.copy(tableType = CatalogTableType.EXTERNAL))
  }

  def addPartitions(table: TableIdentifier, partitionsSpecs: Seq[TablePartitionSpec])(implicit
    spark: SparkSession
  ): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(table)
    val newPartitions = partitionsSpecs.map(partition =>
      CatalogTablePartition(
        partition,
        tableMetadata.storage.copy(locationUri = Some(new URI(tableMetadata.location + toPath(partition))))
      )
    )

    spark.sessionState.catalog.createPartitions(table, newPartitions, ignoreIfExists = true)
  }

  private def toPath(partitionSpec: TablePartitionSpec): String = partitionSpec.foldLeft("") {
    case (acc: String, (partitionCol: String, partitionVal: String)) =>
      s"$acc/$partitionCol=$partitionVal"
  }
}
