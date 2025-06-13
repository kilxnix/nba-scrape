from collections import defaultdict
import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import backoff
import logging
import dataclasses
from nba_data.processor import BoxscoreProcessor
import requests
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for handling datetime/date objects.

    Implementation:
    - Converts datetime to ISO format strings
    - Handles date objects by converting to datetime first
    - Preserves millisecond precision
    - Maintains timezone awareness
    """
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        return super().default(obj)


class BoxScoreFetcher:
    """
    Enhanced BoxScore fetcher with robust datetime handling and error recovery.
    Compatible with existing NBA Analytics Platform infrastructure.
    """

    def __init__(self, conn_string: str):
        """
        Initialize BoxScoreFetcher with database connection and filesystem setup.

        Args:
            conn_string (str): PostgreSQL connection string with SSL config
        """
        self.conn_string = conn_string
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.game_data_path = os.path.join(self.base_path, "game_data")
        os.makedirs(self.game_data_path, exist_ok=True)
        logger.info(f"Initialized BoxScoreFetcher with data path: {self.game_data_path}")
        self.processor = BoxscoreProcessor(conn_string)

    def normalize_date(self, date_value: Union[datetime, date, str]) -> datetime:
        """
        Normalize various date formats into datetime objects for consistent comparison.

        Args:
            date_value: Date input in datetime, date, or string format

        Returns:
            datetime: Normalized datetime object

        Raises:
            ValueError: If string date format is invalid
            TypeError: If date_value is an unsupported type
        """
        try:
            if isinstance(date_value, datetime):
                return date_value
            elif isinstance(date_value, date):
                return datetime.combine(date_value, datetime.min.time())
            elif isinstance(date_value, str):
                try:
                    return datetime.strptime(date_value, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Invalid date string format: {date_value}. Expected landlabda-MM-DD")
            else:
                raise TypeError(f"Unsupported date type: {type(date_value)}")
        except Exception as e:
            logger.error(f"Date normalization error for {date_value}: {str(e)}")
            raise

    @backoff.on_exception(backoff.expo,
                          (psycopg2.Error, psycopg2.OperationalError),
                          max_tries=3,
                          jitter=backoff.full_jitter)
    def get_unprocessed_games(self, team_id: str,
                              cutoff_date: Union[datetime, date, str]) -> List[Dict[str, Any]]:
        """
        Enhanced unprocessed games retrieval with datetime handling.

        Implementation:
        - Converts database timestamps to datetime objects
        - Ensures consistent datetime comparison
        - Processes date strings in query parameters

        Example:
            games = fetcher.get_unprocessed_games('gsw', datetime(2025, 1, 24))
            for game in games:
                print(f"Game date: {game['game_date'].isoformat()}")
        """
        normalized_cutoff = self.normalize_date(cutoff_date)
        logger.info(f"Fetching unprocessed games for {team_id} "
                    f"before {normalized_cutoff}")

        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        WITH team_games AS (
                            SELECT DISTINCT g.*,
                                           g.game_date::timestamp as game_timestamp
                            FROM games g
                            JOIN teams home ON g.home_team_id = home.team_id
                            JOIN teams away ON g.away_team_id = away.team_id
                            WHERE (home.team_id = %s OR away.team_id = %s)
                            AND g.game_date < %s::timestamp
                        )
                        SELECT 
                            g.*,
                            home.team_id as home_team,
                            away.team_id as away_team
                        FROM team_games g
                        JOIN teams home ON g.home_team_id = home.team_id
                        JOIN teams away ON g.away_team_id = away.team_id
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM team_statistics ts
                            WHERE ts.game_id = g.game_id
                            AND (ts.team_id = home.team_id 
                                 OR ts.team_id = away.team_id)
                        )
                        ORDER BY g.game_date ASC
                    """, (team_id, team_id, normalized_cutoff))

                    games = cursor.fetchall()
                    logger.info(f"Found {len(games)} unprocessed games for {team_id}")

                    # Pre-process datetime values before returning
                    processed_games = [
                        self._process_datetime_values(game)
                        for game in games
                    ]
                    return processed_games

        except psycopg2.Error as e:
            logger.error(f"Database error fetching unprocessed games for "
                         f"{team_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching unprocessed games for "
                         f"{team_id}: {str(e)}")
            raise
    def calculate_shooting_stats(self, stats):
        return self.processor.calculate_shooting_stats(stats)  # Use instance variable

    def calculate_possession_stats(self, stats):
        return self.processor.calculate_possession_stats(stats)  # Use instance variable 

    def calculate_metrics_hash(self, basic_stats, shooting_stats, possession_stats):
        return self.processor.calculate_metrics_hash(basic_stats, shooting_stats, possession_stats)  # Use instance variable
    
    def process_team_schedules(self, cutoff_date: datetime = None) -> int:
        """
        Process team schedules with enhanced transaction integrity and connection resilience.
        """
        # Identical SQL query preserved
        sql = """
            SELECT DISTINCT g.*, g.game_id, home.team_id as home_team, away.team_id as away_team
            FROM games g
            JOIN teams home ON g.home_team_id = home.team_id
            JOIN teams away ON g.away_team_id = away.team_id
            WHERE g.game_date < %s::timestamp
            ORDER BY g.game_date ASC
        """

        successful_teams = 0
        processed_games = set()
        conn = None
        
        try:
            # Enhanced connection with monitoring parameters
            conn = psycopg2.connect(
                self.conn_string,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30
            )
            conn.autocommit = False
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, (cutoff_date,))
                games = cursor.fetchall()
                
                # Process each game with consistent transaction boundaries
                for game in games:
                    game_id = game['game_id']
                    home_team_id = game['home_team']
                    away_team_id = game['away_team']
                    event_id = game['event_id']
                    
                    # Skip if already processed in this session
                    if game_id in processed_games:
                        continue
                    
                    # Identical team statistics verification
                    with conn.cursor() as check_cursor:
                        check_cursor.execute(
                            "SELECT team_id FROM team_statistics WHERE game_id = %s", 
                            (game_id,)
                        )
                        team_ids = [row[0] for row in check_cursor.fetchall()]
                    
                    if set(team_ids) == set([home_team_id, away_team_id]):
                        logger.info(f"Skipping game {event_id} as team statistics already exist and match team IDs")
                        continue
                    
                    # Process game with consistent transaction scope
                    try:
                        # Identical ESPN API request
                        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
                        response = requests.get(url)
                        response.raise_for_status()
                        boxscore_data = response.json()
                        
                        # Identical statistics processing
                        self.save_team_stats(conn, game_id, home_team_id, boxscore_data)
                        self.save_team_stats(conn, game_id, away_team_id, boxscore_data)
                        self.save_player_stats(conn, game_id, home_team_id, boxscore_data)
                        self.save_player_stats(conn, game_id, away_team_id, boxscore_data)
                        
                        # Identical player position updates
                        for team_abbr in [home_team_id, away_team_id]:
                            self._update_player_positions(conn, game_id, team_abbr, boxscore_data)
                        
                        # Commit transaction before filesystem operations
                        conn.commit()
                        
                        # Identical filesystem persistence
                        team_folder = os.path.join(self.game_data_path, home_team_id)
                        os.makedirs(team_folder, exist_ok=True)
                        self.process_game(game, team_folder)
                        
                        processed_games.add(game_id)
                        successful_teams += 1
                        logger.info(f"✓ Saved boxscore for game {event_id}")
                        
                    except Exception as game_error:
                        # Explicit rollback on failure
                        conn.rollback()
                        logger.error(f"Failed to process game {event_id}: {game_error}")
                        continue
                        
            return successful_teams
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return 0
            
        finally:
            # Guarantee connection cleanup
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass

    def fetch_and_save_game(self, event_id: str) -> bool:
        """Download a single game's boxscore and persist it."""
        conn = None
        try:
            conn = psycopg2.connect(self.conn_string)
            conn.autocommit = False

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT g.*, home.team_id as home_team, away.team_id as away_team
                    FROM games g
                    JOIN teams home ON g.home_team_id = home.team_id
                    JOIN teams away ON g.away_team_id = away.team_id
                    WHERE g.event_id = %s
                """,
                    (event_id,),
                )
                game = cursor.fetchone()

            if not game:
                logger.error(f"Game {event_id} not found in database")
                return False

            game_id = game["game_id"]
            home_team_id = game["home_team"]
            away_team_id = game["away_team"]

            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
            )
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            boxscore_data = resp.json()

            # Verify required data exists and game is completed
            status_completed = False
            try:
                status_completed = (
                    boxscore_data.get("header", {})
                    .get("competitions", [{}])[0]
                    .get("status", {})
                    .get("type", {})
                    .get("completed")
                )
            except Exception:
                status_completed = False

            if not (
                boxscore_data.get("boxscore")
                and boxscore_data.get("boxscore", {}).get("players")
                and status_completed
            ):
                logger.warning(
                    f"Game {event_id} missing required data or not completed"
                )
                return False

            self.save_team_stats(conn, game_id, home_team_id, boxscore_data)
            self.save_team_stats(conn, game_id, away_team_id, boxscore_data)
            self.save_player_stats(conn, game_id, home_team_id, boxscore_data)
            self.save_player_stats(conn, game_id, away_team_id, boxscore_data)

            for team_abbr in [home_team_id, away_team_id]:
                self._update_player_positions(conn, game_id, team_abbr, boxscore_data)

            conn.commit()

            team_folder = os.path.join(self.game_data_path, home_team_id)
            os.makedirs(team_folder, exist_ok=True)
            self.process_game(game, team_folder)
            return True

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to fetch and save game {event_id}: {e}")
            return False
        finally:
            if conn:
                conn.close()
    def _update_player_positions(self, conn, game_id, team_abbr, boxscore_data):
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT player_name 
                FROM player_statistics 
                WHERE game_id = %s 
                AND team_id = %s 
                AND player_position IS NULL
            """, (game_id, team_abbr))
            players = cursor.fetchall()
            
            if players:
                try:
                    for team in boxscore_data['boxscore']['players']:
                        api_abbrev = team.get('team', {}).get('abbreviation', '').lower()
                        if api_abbrev == team_abbr.lower() or \
                        (team_abbr == 'nyk' and api_abbrev == 'ny') or \
                        (team_abbr == 'gsw' and api_abbrev == 'gs') or \
                        (team_abbr == 'sas' and api_abbrev == 'sa') or \
                        (team_abbr in ['lal', 'lac'] and api_abbrev in ['lal', 'la']):
                            for athlete in team['statistics'][0].get('athletes', []):
                                if athlete.get('didNotPlay'):
                                    continue
                                
                                player_name = athlete['athlete']['displayName']
                                position = athlete['athlete']['position']['abbreviation']
                                
                                cursor.execute("""
                                    UPDATE player_statistics 
                                    SET player_position = %s
                                    WHERE game_id = %s 
                                    AND team_id = %s 
                                    AND player_name = %s
                                    AND player_position IS NULL
                                """, (position, game_id, team_abbr, player_name))
                                
                                if cursor.rowcount > 0:
                                    logger.info(f"Updated position to {position} for {player_name}")
                except Exception as e:
                    logger.error(f"Error updating positions for team {team_abbr}: {e}")
                    raise
    def save_team_stats(self, conn, game_id, team_id, boxscore_data):
        """
        Save team stats using trusted team_id from the games table.
        """
        try:
            matched_team = None
            for team in boxscore_data.get('boxscore', {}).get('teams', []):
                # Use only the team block that matches the trusted team_id
                espn_abbr = team.get('team', {}).get('abbreviation', '').lower()

                if team_id == 'lal' and espn_abbr in ['lal', 'la']:
                    matched_team = team
                    break
                elif team_id == 'lac' and espn_abbr in ['lac', 'la']:
                    matched_team = team
                    break
                elif team_id == 'nyk' and espn_abbr == 'ny':
                    matched_team = team
                    break
                elif team_id == 'gsw' and espn_abbr == 'gs':
                    matched_team = team
                    break
                elif team_id == 'sas' and espn_abbr in ['sa', 'sas']:
                    matched_team = team
                    break
                elif espn_abbr == team_id:
                    matched_team = team
                    break

            if not matched_team:
                logger.error(f"No matching team stats found for team_id: {team_id}")
                return False

            stats = matched_team.get('statistics', [])

            # Extract basic stats
            fg = self.extract_stat(stats, 'fieldGoalsMade-fieldGoalsAttempted', "0-0").split('-')
            tp = self.extract_stat(stats, 'threePointFieldGoalsMade-threePointFieldGoalsAttempted', "0-0").split('-')
            ft = self.extract_stat(stats, 'freeThrowsMade-freeThrowsAttempted', "0-0").split('-')

            basic_stats = {
                'fieldGoalsMade-fieldGoalsAttempted': f"{fg[0]}-{fg[1]}",
                'threePointFieldGoalsMade-threePointFieldGoalsAttempted': f"{tp[0]}-{tp[1]}",
                'freeThrowsMade-freeThrowsAttempted': f"{ft[0]}-{ft[1]}",
                'offensiveRebounds': self.extract_stat(stats, 'offensiveRebounds', "0"),
                'defensiveRebounds': self.extract_stat(stats, 'defensiveRebounds', "0"),
                'assists': self.extract_stat(stats, 'assists', "0"),
                'steals': self.extract_stat(stats, 'steals', "0"),
                'blocks': self.extract_stat(stats, 'blocks', "0"),
                'turnovers': self.extract_stat(stats, 'turnovers', "0")
            }

            shooting_stats = self.calculate_shooting_stats(basic_stats)
            possession_stats = self.calculate_possession_stats(basic_stats)
            metrics_hash = self.calculate_metrics_hash(basic_stats, shooting_stats, possession_stats)
            points = self.derive_points_from_stats(stats)

            values = (
                game_id,
                team_id,
                int(fg[0]), int(fg[1]),
                int(tp[0]), int(tp[1]),
                int(ft[0]), int(ft[1]),
                int(basic_stats['offensiveRebounds']),
                int(basic_stats['defensiveRebounds']),
                int(basic_stats['assists']),
                int(basic_stats['steals']),
                int(basic_stats['blocks']),
                int(basic_stats['turnovers']),
                points,
                shooting_stats.get('efg', 0.0),
                possession_stats.get('possessions', 0.0),
                metrics_hash
            )

            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO team_statistics (
                        game_id, team_id, 
                        field_goals_made, field_goals_attempted,
                        three_pointers_made, three_pointers_attempted,
                        free_throws_made, free_throws_attempted,
                        offensive_rebounds, defensive_rebounds,
                        assists, steals, blocks, turnovers, points,
                        efficiency_rating, possession_count, metrics_hash
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (game_id, team_id) DO NOTHING
                """, values)
                conn.commit()
            return True

        except Exception as e:
            logger.error(f"Error saving team stats for game_id: {game_id}, team_id: {team_id}: {e}")
            raise


    def derive_points_from_stats(self, stats):
        """
        Derives the total points scored from the statistics block.
        """
        try:
            ft = self.extract_stat(stats, 'freeThrowsMade-freeThrowsAttempted', "0-0").split('-')
            tp = self.extract_stat(stats, 'threePointFieldGoalsMade-threePointFieldGoalsAttempted', "0-0").split('-')
            fg = self.extract_stat(stats, 'fieldGoalsMade-fieldGoalsAttempted', "0-0").split('-')

            free_throws = int(ft[0])
            three_pointers = int(tp[0]) * 3
            two_pointers = (int(fg[0]) - int(tp[0])) * 2

            return free_throws + three_pointers + two_pointers
        except Exception as e:
            logger.error(f"Failed to derive points from stats: {e}")
            return 0

    def extract_stat(self, stats, stat_name, default_value="0"):
        """
        Extracts a specific stat from the statistics list.
        """
        try:
            return next((x['displayValue'] for x in stats if x['name'] == stat_name), default_value)
        except Exception as e:
            logger.warning(f"Failed to extract stat {stat_name}: {e}")
            return default_value

    def save_player_stats(self, conn, game_id, team_id, boxscore_data):
        try:
            for team in boxscore_data['boxscore']['players']:
                if team['team']['abbreviation'].lower() == team_id.lower() or \
                   (team_id == 'nyk' and team['team']['abbreviation'].lower() == 'ny') or \
                   (team_id == 'gsw' and team['team']['abbreviation'].lower() == 'gs') or \
                   (team_id == 'sas' and team['team']['abbreviation'].lower() in ['sa', 'sas']) or \
                   (team_id == 'lal' and team['team']['abbreviation'].lower() == 'la') or \
                   (team_id == 'lac' and team['team']['abbreviation'].lower() == 'la'):
                    stat_group = team['statistics'][0]
                    stat_keys = stat_group['keys']

                    for athlete in stat_group.get('athletes', []):
                        if athlete.get('didNotPlay'):
                            continue

                        # Stats extraction
                        raw_stats = athlete['stats']

                        # Parse minutes
                        minutes_str = raw_stats[0]
                        minutes = (float(minutes_str.split(':')[0]) + 
                                   float(minutes_str.split(':')[1]) / 60 
                                   if ':' in minutes_str else float(minutes_str))

                        # Generate stats hash
                        stats_dict = dict(zip(stat_keys, raw_stats))
                        metrics_hash = self.processor.calculate_player_metrics_hash(stats_dict, stat_keys)

                        # Ensure SQL placeholders match values exactly
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO player_statistics (
                                    game_id, team_id, player_name, player_position,
                                    minutes_played, points, rebounds, assists,
                                    steals, blocks, turnovers,
                                    field_goals_made, field_goals_attempted,
                                    three_pointers_made, three_pointers_attempted,
                                    metrics_hash
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                         %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (game_id, team_id, player_name) DO NOTHING
                            """, (
                                game_id,
                                team_id,
                                athlete['athlete']['displayName'],
                                athlete['athlete']['position']['abbreviation'],
                                round(minutes, 1),
                                int(raw_stats[13]),  # points
                                int(raw_stats[6]),   # rebounds
                                int(raw_stats[7]),   # assists
                                int(raw_stats[8]),   # steals
                                int(raw_stats[9]),   # blocks
                                int(raw_stats[10]),  # turnovers
                                int(raw_stats[1].split('-')[0]),  # fg made
                                int(raw_stats[1].split('-')[1]),  # fg attempted
                                int(raw_stats[2].split('-')[0]),  # 3p made
                                int(raw_stats[2].split('-')[1]),  # 3p attempted
                                metrics_hash
                            ))
                            conn.commit() # Added commit after each player insert

                            logger.debug(f"Inserted player stats for {athlete['athlete']['displayName']} with position {athlete['athlete']['position']['abbreviation']}")

        except Exception as e:
            logger.error(f"Error saving player stats: {e}")
            raise

    def process_game(self, game: Dict[str, Any], team_path: str) -> None:
        """
        Process and save game data with robust datetime handling.

        Implementation:
        - Uses custom JSON encoder for datetime serialization
        - Implements atomic file writing
        - Validates JSON structure before saving
        - Handles edge cases like None values and empty data

        Args:
            game: Raw game data dictionary
            team_path: Directory path for saving game files

        Example:
            game_data = {
                'event_id': '401704630',
                'game_date': datetime(2025, 1, 20),
                'teams': [...]
            }
            fetcher.process_game(game_data, '/path/to/team')
        """
        try:
            # Extract event ID with validation
            event_id = str(game.get('event_id', ''))
            if not event_id:
                raise ValueError("Missing event_id in game data")

            # Construct filename with team context
            filename = f"boxscore_{event_id}.json"
            filepath = os.path.join(team_path, filename)

            # Pre-process data to handle any nested datetime objects
            processed_game = self._process_datetime_values(game)

            # Write data atomically using temporary file
            temp_filepath = f"{filepath}.tmp"
            try:
                with open(temp_filepath, 'w') as f:
                    json.dump(
                        processed_game,
                        f,
                        cls=DateTimeEncoder,
                        indent=2,
                        sort_keys=True
                    )
                # Atomic rename for data integrity
                os.replace(temp_filepath, filepath)
                logger.info(f"✓ Saved boxscore for game {event_id}")

            except Exception as e:
                # Cleanup temp file if writing failed
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
                raise

        except Exception as e:
            logger.error(f"Error processing game {game.get('event_id', 'unknown')}: {str(e)}")
            raise
    def _process_datetime_values(self, data: Any) -> Any:
        """
        Recursively process datetime values in nested structures.

        Implementation:
        - Handles nested dictionaries and lists
        - Preserves original data structure
        - Converts all datetime/date objects to ISO strings

        Args:
            data: Any data structure that might contain datetime objects

        Returns:
            Processed data with datetime objects converted to strings

        Example:
            data = {
                'date': datetime(2025, 1, 20),
                'nested': {'another_date': date(2025, 1, 21)}
            }
            processed = fetcher._process_datetime_values(data)
            # Result: {'date': '2025-01-20T00:00:00', 
            #          'nested': {'another_date': '2025-01-21'}}
        """
        if isinstance(data, (dict, defaultdict)):
            return {key: self._process_datetime_values(value)
                    for key, value in data.items()}
        elif isinstance(data, (list, tuple)):
            return [self._process_datetime_values(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        return data
class PlayByPlay:
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        base_path = os.path.dirname(os.path.abspath(__file__))
        self.plays_output_folder = os.path.join(base_path, "plays_data")
        os.makedirs(self.plays_output_folder, exist_ok=True)

    def get_db_connection(self):
        """Get database connection with retry logic"""
        @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=3)
        def connect_with_retry():
            try:
                conn = psycopg2.connect(
                    self.conn_string,
                    sslmode='require',
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                conn.set_session(autocommit=False)
                return conn
            except Exception as e:
                print(f"Database connection error: {e}")
                raise

        return connect_with_retry()

    def create_scoring_frames(self, plays_data):
        """
        Restructures play-by-play data into scoring frames.
        Each frame contains all plays between score changes.

        Args:
            plays_data: List of play-by-play events

        Returns:
            List of frames, where each frame contains plays between score changes
        """
        frames = []
        current_frame = {
            'start_score': {'away': 0, 'home': 0},
            'end_score': {'away': 0, 'home': 0},
            'plays': []
        }

        for play in plays_data:
            current_score = play['score']

            # If this is a scoring play or score changed, start new frame
            if (current_score['away'] != current_frame['end_score']['away'] or 
                current_score['home'] != current_frame['end_score']['home']):

                # Only append frame if it contains plays
                if current_frame['plays']:
                    frames.append(current_frame)

                # Start new frame
                current_frame = {
                    'start_score': {
                        'away': current_frame['end_score']['away'],
                        'home': current_frame['end_score']['home']
                    },
                    'end_score': {
                        'away': current_score['away'],
                        'home': current_score['home']
                    },
                    'plays': []
                }

            # Add play details to current frame
            play_details = {
                'id': play['id'],
                'text': play['text'],
                'clock': play['clock'],
                'period': play['period']['displayValue'],
                'type': play['type']['text']
            }

            # Add coordinates if they exist
            if 'coordinate' in play:
                play_details['coordinate'] = play['coordinate']

            current_frame['plays'].append(play_details)

        # Append final frame if it contains plays
        if current_frame['plays']:
            frames.append(current_frame)

        return frames

    def process_game_files_with_frames(self, data_folder):
        """
        Processes game files and saves play-by-play data organized by scoring frames.

        Args:
            data_folder: Path to the main data folder containing game data.
        """

        processed_files = 0
        conn = self.get_db_connection()
        if not conn:
            print("Could not establish database connection.")
            return

        # Iterate through team folders in game_data
        game_data_folder = os.path.join(data_folder, 'game_data')
        for team_folder in os.listdir(game_data_folder):
            team_path = os.path.join(game_data_folder, team_folder)
            if os.path.isdir(team_path):
                # Iterate through boxscore files in each team folder
                for file_name in os.listdir(team_path):
                    if file_name.startswith("boxscore_") and file_name.endswith(".json"):
                        file_path = os.path.join(team_path, file_name)
                        try:
                            with open(file_path, 'r') as f:
                                data = json.load(f)

                            # Extract event ID and team abbreviation
                            event_id = file_name.split('_')[1].split('.')[0]
                            team_abbrev = team_folder

                            # Fetch game_id from the 'games' table
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT game_id FROM games WHERE event_id = %s", (event_id,))
                                result = cursor.fetchone()
                                if result:
                                    game_id = result[0]
                                else:
                                    print(f"No game_id found for event_id: {event_id}")
                                    continue

                            # Extract and structure play-by-play data
                            plays_data = self.extract_play_by_play(data)

                            if plays_data:
                                # Create scoring frames
                                frames = self.create_scoring_frames(plays_data)

                                # Create output structure
                                output_data = {
                                    'game_id': game_id,
                                    'event_id': event_id,
                                    'team': team_abbrev,
                                    'frames': json.dumps(frames)  # Convert frames to JSON string
                                }

                                # Insert into Supabase
                                try:
                                    with conn.cursor() as cursor:
                                        cursor.execute("""
                                            INSERT INTO plays (game_id, event_id, team, frames)
                                            VALUES (%s, %s, %s, %s)
                                        """, (output_data['game_id'], output_data['event_id'], output_data['team'], output_data['frames']))
                                        conn.commit()
                                        print(f"✓ Inserted play-by-play data for {file_name} into Supabase")
                                        processed_files += 1

                                except Exception as e:
                                    print(f"Error inserting play-by-play data into Supabase: {e}")

                        except Exception as e:
                            print(f"Error processing file {file_path}: {e}")
                            continue

        print(f"\nProcessing complete. Processed {processed_files} files.")
        return processed_files

    def extract_play_by_play(self, json_data):
        """
        Extracts play-by-play data with optimized structure.
        """
        try:
            if 'plays' not in json_data:
                return None

            plays_data = []
            for play in json_data['plays']:
                play_dict = {
                    'id': play.get('id'),
                    'sequenceNumber': play.get('sequenceNumber'),
                    'type': {
                        'id': play['type'].get('id'),
                        'text': play['type'].get('text')
                    },
                    'text': play.get('text'),
                    'period': {
                        'number': play['period'].get('number'),
                        'displayValue': play['period'].get('displayValue')
                    },
                    'clock': play['clock'].get('displayValue'),
                    'score': {
                        'away': play.get('awayScore', 0),
                        'home': play.get('homeScore', 0)
                    },
                    'scoringPlay': play.get('scoringPlay', False),
                    'scoreValue': play.get('scoreValue', 0),
                    'shootingPlay': play.get('shootingPlay', False),
                    'wallclock': play.get('wallclock')
                }

                if 'team' in play:
                    play_dict['team'] = {
                        'id': play['team'].get('id')
                    }

                x_coord = play.get('coordinate', {}).get('x')
                y_coord = play.get('coordinate', {}).get('y')
                if x_coord is not None and y_coord is not None:
                    if x_coord != -214748340 and y_coord != -214748365:
                        play_dict['coordinate'] = {
                            'x': x_coord,
                            'y': y_coord
                        }

                plays_data.append(play_dict)

            return plays_data

        except (KeyError, IndexError, TypeError) as e:
            print(f"Error extracting play-by-play data: {e}")
            return None