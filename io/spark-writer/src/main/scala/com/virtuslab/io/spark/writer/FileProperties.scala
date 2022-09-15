package com.virtuslab.io.spark.writer

import com.virtuslab.io.spark.writer.FileFormat.FileFormat

/** I've tested snappy.parquet and snappy.orc compression ratio on simple structures with 14 flat fields
  * and complex structures with hundreds of fields and nested structures up to 5 levels down.
  *
  * Results:<ul>
  * snappy.parquet + simple structure:  1778MB -> 1270MB (compression ratio  1.40x)<br>
  * snappy.orc + simple structure:      1778MB -> 1223MB (compression ratio  1.45x)<br>
  * snappy.parquet + complex structure: 9100MB -> 878MB  (compression ratio 10.36x)<br>
  * snappy.orc + complex structure:     9100MB -> 761MB  (compression ratio 11.96x)</ul>
  * so why default compression ratio of 10 seems reasonable?
  *
  * Let's analyze 2 edge cases:<ul>
  * 10GB uncompressed complex data (which can be compressed 15x):<br>
  * 10 000MB / 10 = 1 000MB, 1 000MB / 128MB = 8 files, 10 000MB / 15 = 666MB, 666MB / 8 files = 83MB per file<br>
  * 10GB uncompressed simple data (which can be compressed 1.1x):<br>
  * 10 000MB / 10 = 1 000MB, 1 000MB / 128MB = 8 files, 10 000MB / 1.1 = 9 091MB, 9 091MB / 8 files = 1136MB per file</ul>
  *
  * So the smallest possible saved file is around 42MB(84MB > 83MB => 84MB / 2 = 42MB)
  * and the biggest possible saved file is around 1.1GB, both of which are fine.
  *
  * Of course by adjusting these 2 params for each job individually more satisfactory results can be obtained.
  */
case class FileProperties(
  fileFormat: FileFormat,
  compressionRatio: Int = 10,
  averageTargetBytes: Long = 128 * 1000 * 1000
)

object FileFormat extends Enumeration {
  type FileFormat = Value
  val Parquet: FileFormat = Value("parquet")
  val Orc: FileFormat = Value("orc")
  val Json: FileFormat = Value("json")
  val Avro: FileFormat = Value("avro")
}
