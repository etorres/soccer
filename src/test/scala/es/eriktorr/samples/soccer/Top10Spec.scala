package es.eriktorr.samples.soccer

import java.time.LocalDateTime

class Top10Spec extends SetupDataset with DateTimeUtils {
  "test" should "work" in {
    import spark.implicits._
    val matches = DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
    val matchesWithScore = ScoreCalculator().score(matches, august2011)


    matchesWithScore.show(false)


  }

  lazy val august2011: LocalDateTime = LocalDateTime.parse("2011-08-01 00:00:00", dateTimeFormatter)
}
