package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.FileManager.{listDirectories, moveDirectoriesToTargetDirectory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatest.{FlatSpec, Matchers}

class FileManagerSpec extends FlatSpec with SparkContext with Matchers with TableDrivenPropertyChecks {

  implicit val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  it should "list correct directories for a given path" in {
    val testCases: TableFor2[String, Seq[String]] = Table(
      ("path", "expectedFiles"),
      (getPath("/test1"), Seq.empty[String]),
      (getPath("/test1/"), Seq.empty[String]),
      (getPath("/test1/") + "*", Seq.empty[String]),
      (getPath("/test2"), Seq(getAbsPath("/test2/test21"))),
      (getPath("/test2/"), Seq(getAbsPath("/test2/test21"))),
      (getPath("/test2/") + "*", Seq(getAbsPath("/test2/test21")))
    )

    forAll(testCases) { (path: String, expectedDirectories: Seq[String]) =>
      listDirectories(path) should contain theSameElementsAs expectedDirectories
    }
  }

  it should "move directories to target directory" in {
    val baseDir = getPath("/test3/")
    val targetDir = getPath("/test3copy/")
    val testFilesToCreate = Seq("test31/f.json", "test32/g.json", "test32/test321/h.json")
    testFilesToCreate.foreach(file => hdfs.createNewFile(new Path(baseDir + file)))

    moveDirectoriesToTargetDirectory("file:" + baseDir, getAbsPath("/test3copy/"))

    testFilesToCreate.foreach(file => hdfs.exists(new Path(targetDir + file)) shouldBe true)
    hdfs.delete(new Path(targetDir + "test31/"), true)
    hdfs.delete(new Path(targetDir + "test32/"), true)
  }

  private def getAbsPath(path: String) = "file:" + getPath(path)

  private def getPath(path: String) = this.getClass.getResource(path).getPath
}
