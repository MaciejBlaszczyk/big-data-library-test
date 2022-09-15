package com.virtuslab.io.spark.writer

import org.apache.spark.sql.DataFrame

object DropDuplicatesMergePolicy {
  case class ExcludeColumns(excludeCols: Seq[String]) extends DropDuplicatesMergePolicy {
    def getColumns(dataFrame: DataFrame): Array[String] = {
      dataFrame.columns.filterNot(col => excludeCols.contains(col))
    }
  }

  case class SelectedColumnsBasedOnOrdering(selectedCols: Seq[String], orderCols: Seq[String])
      extends DropDuplicatesMergePolicy {
    def getColumns(dataFrame: DataFrame): Array[String] = {
      dataFrame.columns
    }
  }

  case class AllColumns() extends DropDuplicatesMergePolicy {
    def getColumns(dataFrame: DataFrame): Array[String] = {
      dataFrame.columns
    }
  }
}

trait DropDuplicatesMergePolicy {
  def getColumns(dataFrame: DataFrame): Array[String]
}
