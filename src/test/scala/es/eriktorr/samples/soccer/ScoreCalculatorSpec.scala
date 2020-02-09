package es.eriktorr.samples.soccer

import java.time.LocalDateTime

import org.apache.spark.sql.Dataset

class ScoreCalculatorSpec extends SetupDataset with DateTimeUtils {
  "Score calculator" should "add team scores to matches that happened after a given date" in {
    import spark.implicits._
    val matches = DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
    val matchesWithScore = ScoreCalculator().score(matches, august2011)
    matchesWithScore.count() shouldBe 16124
    assertDatasetEquals(expectedMatches(), matchesWithScore.orderBy('date).limit(1))
  }

  private def expectedMatches(): Dataset[MatchWithScore] = {
    import spark.implicits._
    Seq(MatchWithScore(id = 16446,
      country_id = 15722,
      league_id = 15722,
      date = timestampFrom("2011-08-01 00:00:00"),
      match_api_id = 1030794,
      home_team_api_id = 8033,
      away_team_api_id = 1957,
      home_team_goal = 2,
      away_team_goal = 2,
      home_team_score = 1,
      away_team_score = 1)).toDF.as[MatchWithScore]
  }

  lazy val august2011: LocalDateTime = LocalDateTime.parse("2011-08-01 00:00:00", dateTimeFormatter)
}
