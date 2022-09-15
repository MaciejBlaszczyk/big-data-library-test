package com.virtuslab.io.spark.writer
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

final case class WriteResult(paths: Seq[String], count: Long, fileCount: Int)

object WriteResult {
  implicit val encoder: Encoder[WriteResult] = deriveEncoder[WriteResult]
  implicit val decoder: Decoder[WriteResult] = deriveDecoder[WriteResult]
}
