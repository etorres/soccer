package es.eriktorr.samples.soccer

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  lazy implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
}

object SparkSessionProvider {
  def sparkSession: SparkSession = SparkSession.builder
    .appName("soccer")
    .master("local[*]")
    .getOrCreate()
}