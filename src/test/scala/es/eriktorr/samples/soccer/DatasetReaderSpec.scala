package es.eriktorr.samples.soccer

class DatasetReaderSpec extends SetupDataset {
  "Dataset reader" should "load countries from CSV file" in {
    val countries = DatasetReader[Country].datasetFrom(pathToFile("data/country.csv.bz2"))
    assertDatasetEquals(expectedCountries(), countries)
  }

  private def expectedCountries() = {
    import spark.implicits._
    Seq(Country(1, "Belgium"),
      Country(1729, "England"),
      Country(4769, "France"),
      Country(7809, "Germany"),
      Country(10257, "Italy"),
      Country(13274, "Netherlands"),
      Country(15722, "Poland"),
      Country(17642, "Portugal"),
      Country(19694, "Scotland"),
      Country(21518, "Spain"),
      Country(24558, "Switzerland")).toDF.as[Country]
  }
}
