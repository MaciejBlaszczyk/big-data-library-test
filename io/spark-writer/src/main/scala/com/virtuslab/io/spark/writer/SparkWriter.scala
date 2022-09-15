package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.DropDuplicatesMergePolicy.SelectedColumnsBasedOnOrdering
import com.virtuslab.io.spark.writer.FileManager.{moveDirectoriesToTargetDirectory, removeDirectory}
import com.virtuslab.io.spark.writer.TableMetadataManager._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.util.UUID
import scala.collection.mutable
import scala.util.Random

object SparkWriter {
  type PartitionRange = (String, (Int, Int))
  type PartitionsRangeMap = Map[String, (Int, Int)]
  val partitionsCombined = "partitions_combined"
  val additionalPartition = "additional_partition"

  def write(
    df: DataFrame,
    partitionCols: Seq[String],
    table: TableIdentifier,
    tableLocation: String,
    fileProps: FileProperties,
    dfCount: Long
  ): WriteResult = {
    implicit val spark: SparkSession = df.sparkSession
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val t = new TaskMetricsExplorer(df.sparkSession)
    val partitionColsOrdered = getPartitionColsInOrder(table, partitionCols)
    val (partitionsUniqueSpecs, metrics) = t.runAndMeasure(getUniquePartitionsSpecs(df, partitionColsOrdered))

    val dfWithCombinedPartitions = df
      .withColumn(partitionsCombined, concat_ws("___", partitionCols.map(col(_).cast(StringType)): _*))
      .cache()

    val optimalNumberOfFiles =
      calculateOptimalNumberOfFiles(metrics, fileProps.compressionRatio, fileProps.averageTargetBytes)
    val partitionsRanges = calculatePartitionsRanges(dfWithCombinedPartitions, optimalNumberOfFiles, dfCount)
    val addAdditionalPartitionNumber = createAddAdditionalPartitionNumberUdf(partitionsRanges)

    val tempDirectory = s"$tableLocation/spark_writer_staging_dir_${UUID.randomUUID().toString}"
    dfWithCombinedPartitions
      .withColumn(additionalPartition, addAdditionalPartitionNumber(col(partitionsCombined)))
      .repartitionByRange(col(additionalPartition))
      .drop(partitionsCombined, additionalPartition)
      .write
      .partitionBy(partitionColsOrdered: _*)
      .format(fileProps.fileFormat.toString)
      .save(tempDirectory)

    dropPartitions(table, partitionsUniqueSpecs)
    val movedPaths = moveDirectoriesToTargetDirectory(tempDirectory, tableLocation)
    addPartitions(table, partitionsUniqueSpecs)
    removeDirectory(tempDirectory, recursive = true)
    WriteResult(movedPaths, dfCount, optimalNumberOfFiles)
  }

  def mergePartitions(
    df: DataFrame,
    partitionCols: Seq[String],
    table: TableIdentifier,
    mergePolicy: DropDuplicatesMergePolicy,
    additionalFilterForExistingData: Column
  ): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    val partitionColsOrdered = getPartitionColsInOrder(table, partitionCols)
    val partitionsUniqueSpecs =
      getUniquePartitionsSpecs(df, partitionColsOrdered)
    val mergedDf = if (tableExists(table) && partitionsUniqueSpecs.nonEmpty) {
      spark.read
        .table(table.toString)
        .filter(createPartitionFilter(partitionsUniqueSpecs))
        .filter(additionalFilterForExistingData)
        .unionByName(df)
    } else {
      df
    }

    mergePolicy match {
      case SelectedColumnsBasedOnOrdering(partitionsColumns, orderColumns) =>
        val rowNumber = "row_number"
        val window = Window.partitionBy(partitionsColumns.map(col): _*).orderBy(orderColumns.map(col): _*)
        mergedDf
          .withColumn(rowNumber, row_number().over(window))
          .filter(col(rowNumber) === lit(1))
          .drop(col(rowNumber))
      case _ => mergedDf.dropDuplicates(mergePolicy.getColumns(df))
    }
  }

  private def createAddAdditionalPartitionNumberUdf(
    partitionsRanges: PartitionsRangeMap
  )(implicit spark: SparkSession): UserDefinedFunction = {
    val partitionsRangesBroadcast = spark.sparkContext.broadcast(partitionsRanges)

    udf { partitionsCombined: String =>
      val r = new Random()
      val (rangeStart, rangeEnd) = partitionsRangesBroadcast.value(partitionsCombined)
      r.nextInt(rangeEnd - rangeStart) + rangeStart
    }
  }

  private def calculateOptimalNumberOfFiles(
    metrics: mutable.Buffer[TaskInfoMetrics],
    compressionRatio: Int,
    averageOptimalBytesPerFile: Long
  ): Int = {
    val uncompressedBytesRead = findTheMostReadBytesInSingleStage(metrics)
    ((uncompressedBytesRead / compressionRatio) / averageOptimalBytesPerFile).ceil.toInt
  }

  private def findTheMostReadBytesInSingleStage(metrics: mutable.Buffer[TaskInfoMetrics]): Double =
    metrics.groupBy(_.stageId).mapValues(_.map(_.bytesRead).sum).values.max.toDouble

  private def calculatePartitionsRanges(df: DataFrame, optimalNumberOfFiles: Int, dfCount: Long): PartitionsRangeMap = {
    import df.sparkSession.implicits._

    df.groupBy(partitionsCombined)
      .count()
      .as[PartitionCount]
      .collect()
      .map(_.toPartitionFiles(optimalNumberOfFiles, dfCount))
      .scanLeft(("toBeDropped", (0, 0)))(calculatePartitionRange)
      .drop(1)
      .toMap
  }

  private def calculatePartitionRange: (PartitionRange, PartitionFiles) => PartitionRange = {
    case ((_: String, (_: Int, previousRangeEnd: Int)), pf: PartitionFiles) =>
      val rangeStart = previousRangeEnd
      val rangeEnd = previousRangeEnd + pf.files
      pf.partition -> (rangeStart -> rangeEnd)
  }

  private def createPartitionFilter(partitionsUniqueSpecs: Seq[TablePartitionSpec]): Column = {
    partitionsUniqueSpecs
      .map {
        _.map { case (partitionName, partitionValue) => col(partitionName) === partitionValue }
          .reduceLeft(_ and _)
      }
      .reduceLeft(_ or _)
  }

  private def getUniquePartitionsSpecs(df: DataFrame, partitionCols: Seq[String]): Seq[TablePartitionSpec] = {
    df.rdd //do not simplify to _.getValuesMap[String] it DOESN'T work (eg. integers are still integers and they fail later)
      .map(_.getValuesMap[Any](partitionCols))
      .distinct()
      .collect()
      .toSeq
      .map(
        partitions =>
          partitions.map { case (partitionCol: String, partitionVal: Any) => (partitionCol, partitionVal.toString) }
      )
  }

  final case class PartitionFiles(partition: String, files: Int)

  final case class PartitionCount(partitions_combined: String, count: Long) {
    def toPartitionFiles(optimalNumberOfFiles: Int, dfCount: Long): PartitionFiles = {
      val optimalNumberOfFilesForPartition = ((count.toDouble / dfCount) * optimalNumberOfFiles).ceil.toInt
      PartitionFiles(partitions_combined, optimalNumberOfFilesForPartition)
    }
  }
}
