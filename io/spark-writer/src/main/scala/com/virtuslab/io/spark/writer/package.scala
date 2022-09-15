package com.virtuslab.io.spark

import com.virtuslab.io.spark.writer.DropDuplicatesMergePolicy.AllColumns
import com.virtuslab.io.spark.writer.TableMetadataManager.{createEmptyExternalTable, extractTableLocation, tableExists}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

package object writer {

  implicit class RichDataFrame(df: DataFrame) {
    def writeAndMergePartitions(
      partitionCols: List[String],
      table: TableIdentifier,
      mergePolicy: DropDuplicatesMergePolicy = AllColumns(),
      fileProps: FileProperties = FileProperties(FileFormat.Parquet),
      additionalFilterForExistingData: Column = lit(true)
    ): WriteResult = {
      writePartitions(
        partitionCols,
        table,
        () => SparkWriter.mergePartitions(df, partitionCols, table, mergePolicy, additionalFilterForExistingData),
        fileProps
      )
    }

    def writePartitions(
      partitionCols: Seq[String],
      table: TableIdentifier,
      mergedDataFrame: () => DataFrame = () => df,
      fileProps: FileProperties = FileProperties(FileFormat.Parquet)
    ): WriteResult = {
      implicit val spark: SparkSession = df.sparkSession
      val mergedDf = mergedDataFrame()
      val dfCount = mergedDf.cache().count()

      if (dfCount > 0) {
        val tableLocation = extractTableLocation(table)
        if (!tableExists(table))
          createEmptyExternalTable(mergedDf, partitionCols, table, tableLocation, fileProps.fileFormat)

        SparkWriter.write(mergedDf, partitionCols, table, tableLocation, fileProps, dfCount)
      } else {
        WriteResult(List.empty, 0, 0)
      }
    }
  }
}
