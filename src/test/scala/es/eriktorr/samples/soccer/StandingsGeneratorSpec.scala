package es.eriktorr.samples.soccer

import java.time.LocalDateTime
import org.apache.spark.sql.functions._

class StandingsGeneratorSpec extends SetupDataset with DateTimeUtils {
  "test" should "work" in {
    import spark.implicits._
    val matches = DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
    val matchesWithScore = ScoreCalculator().score(matches, august2011)

    val home_team = matchesWithScore.select('country_id, 'league_id, 'season, 'date, 'home_team_api_id, 'home_team_goal, 'home_team_score)
        .withColumnRenamed("home_team_api_id", "team_api_id")
        .withColumnRenamed("home_team_goal", "team_goal")
        .withColumnRenamed("home_team_score", "team_score")

    val away_team = matchesWithScore.select('country_id, 'league_id, 'season, 'date, 'away_team_api_id, 'away_team_goal, 'away_team_score)
      .withColumnRenamed("away_team_api_id", "team_api_id")
      .withColumnRenamed("away_team_goal", "team_goal")
      .withColumnRenamed("away_team_score", "team_score")

    home_team.union(away_team)
      .cube('country_id, 'league_id, 'season, 'team_api_id)
      .agg(count('team_api_id) as "played", count(when('team_score === 3, 1)) as "won",
        count(when('team_score === 1, 1)) as "drawn",
        count(when('team_score === 0, 1)) as "lost",
        sum('team_goal) as "goals_for", sum('team_score) as "points")
      .filter('country_id.isNotNull && 'league_id.isNotNull && 'season.isNotNull && 'team_api_id.isNotNull)
      .sort('season.desc_nulls_first, 'points.desc_nulls_first)
      .where('league_id === 21518)
      .show(21, truncate = false)


  }

  lazy val august2011: LocalDateTime = LocalDateTime.parse("2011-08-01 00:00:00", dateTimeFormatter)
}
