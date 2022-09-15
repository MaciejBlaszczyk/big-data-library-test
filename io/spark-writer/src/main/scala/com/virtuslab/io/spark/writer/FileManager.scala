package com.virtuslab.io.spark.writer

import org.apache.hadoop.fs.{FileSystem, Path}

object FileManager {
  def moveDirectoriesToTargetDirectory(sourceDir: String, targetDir: String)(implicit fs: FileSystem): Seq[String] = {
    listDirectories(sourceDir).map { tempDir =>
      val relativePath = tempDir.replace(sourceDir, "")
      val targetPath = new Path(targetDir + "/" + relativePath)
      fs.rename(new Path(tempDir), targetPath)
      targetPath.toUri.getPath
    }
  }

  def listDirectories(path: String)(implicit fs: FileSystem): Seq[String] = {
    Option(fs.globStatus(new Path(adjustPathToEndWithSlashAsterisk(path)))) match {
      case Some(fileStatuses) if fileStatuses.nonEmpty =>
        fileStatuses.filter(_.isDirectory).map(_.getPath.toString).toSeq
      case _ =>
        Seq.empty[String]
    }
  }

  private def adjustPathToEndWithSlashAsterisk(path: String): String = {
    if (path.endsWith("*")) path
    else if (path.endsWith("/")) path + "*"
    else path + "/*"
  }

  def removeDirectory(path: String, recursive: Boolean)(implicit fs: FileSystem): Unit = {
    fs.delete(new Path(path), recursive)
  }
}
