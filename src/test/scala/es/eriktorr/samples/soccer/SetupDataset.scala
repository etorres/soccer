package es.eriktorr.samples.soccer

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

abstract class SetupDataset extends FlatSpec with Matchers with DatasetSuiteBase {
  override implicit def enableHiveSupport: Boolean = false

  implicit def sparkSession: SparkSession = spark

  val pathToFile: String => String = {
    getClass.getClassLoader.getResource(_).getPath
  }
}
