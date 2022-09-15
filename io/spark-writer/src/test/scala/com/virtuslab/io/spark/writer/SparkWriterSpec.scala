package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.SparkWriter._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor4}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, PrivateMethodTester}

import scala.collection.mutable
import scala.util.Random

case class NoiseDto(partition_number: Int, noise: String)

case class ShortDto(num: Int, letter: String)

case class PartDto(partitions_combined: String, num: Int)

class SparkWriterSpec
    extends FlatSpec
    with Matchers
    with SparkContext
    with BeforeAndAfterAll
    with PrivateMethodTester
    with TableDrivenPropertyChecks {
  import spark.implicits._

  implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val tableName = "spark_writer_test_table"
  val dbName = "spark_writer_test_db"

  override protected def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  }
  override protected def afterAll(): Unit = {}

  it should "save data with one small and one big partition as 1 file for small partition and 9 files for big partition" in {
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    val fileProps =
      FileProperties(fileFormat = FileFormat.Parquet, compressionRatio = 1, averageTargetBytes = 128 * 1000)
    val df = (generateDtos(81000, 1) ++ generateDtos(500, 2)).toDF

    val res =
      df.writePartitions(partitionCols = Seq("partition_number"), table = tableIdentifier, fileProps = fileProps)
    val savedNumberOfFiles = res.paths.map(path => fs.listStatus(new Path(path)).count(_.isFile))

    val expectedNumberOfFiles = Seq(1, 9)
    savedNumberOfFiles should contain theSameElementsAs expectedNumberOfFiles

  }

  it should "create correct partition filter from unique partition specs" in {
    val testCases: TableFor2[Seq[Map[String, String]], Column] = Table(
      ("partitionUniqueValues", "expectedPartitionFilter"),
      (Seq(Map("num" -> "1")), 'num === "1"),
      (Seq(Map("num" -> "1"), Map("num" -> "2")), 'num === "1" or 'num === "2"),
      (Seq(Map("num" -> "1", "letter" -> "A")), 'num === "1" and 'letter === "A"),
      (
        Seq(Map("num" -> "1", "letter" -> "A"), Map("num" -> "1", "letter" -> "B")),
        ('num === "1" and 'letter === "A") or ('num === "1" and 'letter === "B")
      )
    )

    val createPartitionFilters = PrivateMethod[Column]('createPartitionFilter)
    forAll(testCases) { (partitionUniqueValues: Seq[Map[String, String]], expectedPartitionFilter: Column) =>
      (SparkWriter invokePrivate createPartitionFilters(partitionUniqueValues)) shouldBe expectedPartitionFilter
    }
  }

  it should "get unique partition specs from a dataframe" in {
    val df = Seq(ShortDto(1, "A"), ShortDto(1, "A"), ShortDto(1, "B"), ShortDto(2, "B"), ShortDto(2, "B")).toDF

    val getUniquePartitionValues = PrivateMethod[Seq[Map[String, String]]]('getUniquePartitionsSpecs)
    val uniquePartitionValues = SparkWriter invokePrivate getUniquePartitionValues(df, Seq("num", "letter"))

    val expectedUniquePartitionValues =
      Seq(Map("num" -> "1", "letter" -> "A"), Map("num" -> "1", "letter" -> "B"), Map("num" -> "2", "letter" -> "B"))
    uniquePartitionValues should contain theSameElementsAs expectedUniquePartitionValues
  }

  it should "calculate optimal number of files for given partitions" in {
    val testCases: TableFor4[Int, Long, Long, PartitionFiles] = Table(
      ("optimalNumberOfFiles", "dfCount", "partitionCount", "expectedPartitionFiles"),
      (10, 1000, 1, PartitionFiles("part", 1)),
      (10, 1000, 99, PartitionFiles("part", 1)),
      (10, 1000, 100, PartitionFiles("part", 1)),
      (10, 1000, 101, PartitionFiles("part", 2)),
      (10, 1000, 1000, PartitionFiles("part", 10)),
      (1, 1000, 1, PartitionFiles("part", 1)),
      (1, 1000, 999, PartitionFiles("part", 1)),
      (1, 1000, 1000, PartitionFiles("part", 1))
    )

    forAll(testCases) {
      (optimalNumberOfFiles: Int, dfCount: Long, count: Long, expectedPartitionFiles: PartitionFiles) =>
        PartitionCount("part", count).toPartitionFiles(optimalNumberOfFiles, dfCount) shouldBe expectedPartitionFiles
    }
  }

  it should "calculate correct partitions' ranges for df" in {
    val df = Seq(PartDto("A", 1), PartDto("A", 2), PartDto("B", 3), PartDto("C", 4), PartDto("C", 5)).toDF
    val optimalNumberOfFiles = 5
    val dfCount = df.count()

    val calculatePartitionsRanges = PrivateMethod[PartitionsRangeMap]('calculatePartitionsRanges)
    val partitionsRanges = SparkWriter invokePrivate calculatePartitionsRanges(df, optimalNumberOfFiles, dfCount)

    val expectedPartitionsRanges = Map("C" -> (0 -> 2), "B" -> (2 -> 3), "A" -> (3 -> 5))
    partitionsRanges shouldBe expectedPartitionsRanges
  }

  it should "calculate optimal number of files based on job metrics" in {
    val metrics = mutable.Buffer(TaskInfoMetrics(1, 1000), TaskInfoMetrics(1, 1001), TaskInfoMetrics(2, 0))
    val compressionRatio = 10
    val averageOptimalBytesPerFile = 128L

    val calculateOptimalNumberOfFiles = PrivateMethod[Int]('calculateOptimalNumberOfFiles)
    val numberOfFiles =
      SparkWriter invokePrivate calculateOptimalNumberOfFiles(metrics, compressionRatio, averageOptimalBytesPerFile)

    val expectedNumberOfFiles = 2
    numberOfFiles shouldBe expectedNumberOfFiles
  }

  private def genString(n: Int): String = Random.alphanumeric.filter(_.isLetter).take(n).mkString

  private def generateDtos(n: Int, partition_number: Int): Seq[NoiseDto] =
    (1 to n).map(_ => NoiseDto(partition_number, genString(10)))
}
