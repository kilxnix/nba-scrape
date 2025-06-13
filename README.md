# NBA Analytics API

This project provides a Flask-based API for scraping NBA data and performing various statistical analyses. Below is a summary of the available API endpoints and their HTTP methods.

## Endpoints

| Endpoint | Method | Description |
| --- | --- | --- |
| `/fetch_schedules` | `POST` | Download schedules for all teams or a specific team. |
| `/fetch_date_range_schedules` | `POST` | Fetch schedules within a given date range. |
| `/team_schedule/<team_abbr>` | `GET` | Retrieve the schedule for a single team. |
| `/todays_games` | `GET` | List the games scheduled for today. |
| `/fetch_boxscores` | `POST` | Download and store available boxscore data. |
| `/update_team_data/<team_abbr>` | `POST` | Fetch a team's schedule, download its boxscores, and store them. **This endpoint must be called using HTTP POST.** |
| `/fetch_plays` | `POST` | Process play-by-play data from local files. |
| `/process_stats` | `POST` | Run statistical processing on stored boxscores. |
| `/analyze_matchups` | `GET` | Generate matchup analysis for upcoming or recent games. |

These endpoints allow you to update local data, process statistics, and analyze upcoming games. Each one returns a JSON response describing the operation's result.

