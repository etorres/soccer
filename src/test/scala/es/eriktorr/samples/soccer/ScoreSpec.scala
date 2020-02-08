package es.eriktorr.samples.soccer

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class ScoreSpec extends SetupDataset {
  "test" should "work" in {
    import spark.implicits._

    DatasetReader[Match].datasetFrom(pathTo("data/match.csv.bz2"))
      .withColumn("score", scoreFrom('away_team_goal, 'home_team_goal))
      .select('id, 'country_id, 'league_id, 'date, 'home_team_api_id, 'away_team_api_id, 'home_team_goal, 'away_team_goal, $"score._1" as "home_team_score", $"score._2" as "away_team_score")
      .filter('date >= Timestamp.valueOf("2011-08-01 00:00:00"))
      .show(true)

    // TODO

  }

  lazy val scoreFrom: UserDefinedFunction = udf((goalTeamA: Int, goalTeamB: Int) => (goalTeamA, goalTeamB) match {
    case (goalTeamA, goalTeamB) if goalTeamA > goalTeamB => (3, 0)
    case (goalTeamA, goalTeamB) if goalTeamA < goalTeamB => (0, 3)
    case _ => (1, 1)
  })
}
