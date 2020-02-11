package es.eriktorr.samples.soccer

import java.time.LocalDateTime

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class StandingsGeneratorSpec extends SetupDataset with DateTimeUtils {
  "test" should "work" in {
    import spark.implicits._
    val matches = DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
    val matchesWithScore = ScoreCalculator()
      .score(matches, august2000)
      .cache()

    val home_team = matchesWithScore.select('league_id, 'season, 'home_team_api_id, 'home_team_goal, 'away_team_goal, 'home_team_score)
      .withColumnRenamed("home_team_api_id", "team_api_id")
      .withColumnRenamed("home_team_goal", "team_goal")
      .withColumnRenamed("away_team_goal", "against_goal")
      .withColumnRenamed("home_team_score", "team_score")

    val away_team = matchesWithScore.select('league_id, 'season, 'away_team_api_id, 'away_team_goal, 'home_team_goal, 'away_team_score)
      .withColumnRenamed("away_team_api_id", "team_api_id")
      .withColumnRenamed("away_team_goal", "team_goal")
      .withColumnRenamed("home_team_goal", "against_goal")
      .withColumnRenamed("away_team_score", "team_score")

    val standings = home_team.union(away_team)
      .repartition('league_id)
      .cube('league_id, 'season, 'team_api_id)
      .agg(count('team_api_id) as "played",
        count(when('team_score === 3, 1)) as "won",
        count(when('team_score === 1, 1)) as "drawn",
        count(when('team_score === 0, 1)) as "lost",
        sum('team_goal) as "goals_for",
        sum('against_goal) as "goals_against",
        sum('team_score) as "points")
      .withColumn("goal_difference", 'goals_for - 'goals_against)
      .filter('league_id.isNotNull && 'season.isNotNull && 'team_api_id.isNotNull)
      .sort('season.desc_nulls_first, 'points.desc_nulls_first)
      .withColumn("auto_inc", monotonically_increasing_id())

    val teams = DatasetReader[Team].datasetFrom(pathTo("data/team.csv.bz2"))

    val positionColumn = row_number().over(Window.partitionBy('league_id).orderBy('auto_inc)).alias("position")

    standings
      .join(teams, Seq("team_api_id"))
      .select(positionColumn, 'team_long_name as "team", 'played, 'won, 'drawn, 'lost, 'goals_for, 'goals_against, 'goal_difference, 'points)
      .where('league_id === 21518 && 'season === "2015/2016")
      .show(20, truncate = false)

  }

  lazy val august2000: LocalDateTime = LocalDateTime.parse("2000-08-01 00:00:00", dateTimeFormatter)
}
