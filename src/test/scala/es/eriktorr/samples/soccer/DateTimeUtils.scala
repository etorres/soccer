package es.eriktorr.samples.soccer

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

trait DateTimeUtils {
  val timestampFrom: String => Timestamp = (str: String) => Timestamp.valueOf(str)

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}
