package es.eriktorr.samples.soccer

import java.sql.Timestamp

case class MatchWithScore(id: Int,
                          country_id: Int,
                          league_id: Int,
                          date: Timestamp,
                          match_api_id: Int,
                          home_team_api_id: Int,
                          away_team_api_id: Int,
                          home_team_goal: Int,
                          away_team_goal: Int,
                          home_team_score: Int,
                          away_team_score: Int)
