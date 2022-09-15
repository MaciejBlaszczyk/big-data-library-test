package com.virtuslab.io.spark.writer

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class TaskInfoRecorderListener extends SparkListener with Serializable {
  val taskInfoMetrics: mutable.Buffer[TaskInfoMetrics] = mutable.Buffer[TaskInfoMetrics]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    if (taskEnd.taskInfo != null && taskEnd.taskMetrics != null) {
      taskInfoMetrics += TaskInfoMetrics(taskEnd.stageId, taskEnd.taskMetrics.inputMetrics.bytesRead)
    }
  }
}

class TaskMetricsExplorer(sparkSession: SparkSession) extends Serializable {
  val listenerTask = new TaskInfoRecorderListener()
  sparkSession.sparkContext.addSparkListener(listenerTask)

  def runAndMeasure[T](f: => T): (T, mutable.Buffer[TaskInfoMetrics]) = {
    val fResult = f
    (fResult, listenerTask.taskInfoMetrics)
  }
}

final case class TaskInfoMetrics(stageId: Int, bytesRead: Long)
