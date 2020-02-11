package es.eriktorr.samples.soccer

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.Dataset

class ScoreCalculatorSpec extends SetupDataset with DateTimeUtils {
  "Score calculator" should "add team scores to matches that happened after a given date" in {
    import spark.implicits._
    val matches = DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
    val matchesWithScore = ScoreCalculator().score(matches)
      .filter('date >= Timestamp.valueOf(may2016))
      .orderBy('date.desc_nulls_first)

    matchesWithScore.count() shouldBe 209
    assertDatasetEquals(expectedMatches(), matchesWithScore.limit(1))
  }

  private def expectedMatches(): Dataset[MatchWithScore] = {
    import spark.implicits._
    Seq(MatchWithScore(id = 25945,
      country_id = 24558,
      league_id = 24558,
      season = "2015/2016",
      date = timestampFrom("2016-05-25 00:00:00"),
      match_api_id = 1992225,
      home_team_api_id = 9931,
      away_team_api_id = 9956,
      home_team_goal = 0,
      away_team_goal = 1,
      home_team_score = 0,
      away_team_score = 3)).toDF.as[MatchWithScore]
  }

  lazy val may2016: LocalDateTime = LocalDateTime.parse("2016-05-01 00:00:00", dateTimeFormatter)
}
