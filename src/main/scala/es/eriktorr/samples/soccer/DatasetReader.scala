package es.eriktorr.samples.soccer

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{Dataset, Encoders}

object DatasetReader {
  def apply[A <: Product : TypeTag]: DatasetReader[A] = new DatasetReader[A]
}

class DatasetReader[A <: Product : TypeTag] extends SparkSessionProvider {
  def datasetFrom(pathToFile: String): Dataset[A] = {
    import spark.implicits._
    val schema = Encoders.product[A].schema
    spark.read.option("header", value = true).schema(schema).csv(pathToFile).as[A]
  }
}
