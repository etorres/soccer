package es.eriktorr.samples.soccer

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object ScoreCalculator {
  def apply(): ScoreCalculator = new ScoreCalculator()
}

class ScoreCalculator extends SparkSessionProvider {
  def score(matches: Dataset[Match], startingDate: LocalDateTime): Dataset[MatchWithScore] = {
    import spark.implicits._
    matches
      .withColumn("score", udfScore('home_team_goal, 'away_team_goal))
      .select('id, 'country_id, 'league_id, 'season, 'date, 'match_api_id, 'home_team_api_id, 'away_team_api_id,
        'home_team_goal, 'away_team_goal, $"score._1" as "home_team_score", $"score._2" as "away_team_score")
      .filter('date >= Timestamp.valueOf(startingDate))
      .as[MatchWithScore]
  }

  val scoreFrom: (Int, Int) => (Int, Int) = (x: Int, y: Int) => (x, y) match {
    case (x, y) if x > y => (3, 0)
    case (x, y) if x < y => (0, 3)
    case _ => (1, 1)
  }

  lazy val udfScore: UserDefinedFunction = udf[(Int, Int), Int, Int](scoreFrom)
}
