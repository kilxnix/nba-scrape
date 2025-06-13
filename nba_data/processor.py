import json
import os
import glob
import re
from collections import defaultdict
from typing import Dict, Any, List
import psycopg2
import hashlib
import logging

class BoxscoreProcessor:
    def __init__(self, conn_string: str):
        self.setup_logging()
        self.conn_string = conn_string
        base_path = os.path.dirname(os.path.abspath(__file__))
        self.game_data_path = os.path.join(base_path, "game_data")
        self.stats_path = os.path.join(base_path, "game_stats")
        os.makedirs(self.stats_path, exist_ok=True)

    def setup_logging(self):
        """Configure detailed logging for tracking operations and debugging."""
        log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nba_schedule.log')
        logging.basicConfig(
            filename=log_file_path,
            filemode='w',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info('Initializing NBA Schedule Fetcher')

    def get_db_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.conn_string)
        except Exception as e:
            print(f"Error connecting to the database: {e}")
        return conn

    def _extract_stat_from_list(self, stats_list: List[Dict], stat_name: str, default: str = "0") -> str:
        """Helper method to extract a stat's display value from a list of stat dictionaries."""
        try:
            return next((item.get('displayValue', default)
                         for item in stats_list if item.get('name') == stat_name), default)
        except Exception as e:
            logging.error(f"Error extracting stat {stat_name}: {str(e)}")
            return default

    def extract_team_stats(self, data: dict) -> Dict[str, Dict]:
        """
        Extract team statistics.
        First, attempt to use the new JSON structure (metadata + team_stats/raw);
        if not found, fall back to the legacy structure from the "boxscore" key.
        """
        team_stats = {}
        try:
            # Get team identifier from metadata (if available)
            team_abbrev = data.get('metadata', {}).get('team', '').lower()
            if team_abbrev == "gs":
                team_abbrev = "gsw"

            # Try new structure first
            raw_stats = data.get('team_stats', {}).get('raw', {})
            if not raw_stats:
                logging.warning(f"No raw stats found in 'team_stats' for team {team_abbrev}. "
                                "Attempting legacy extraction.")
                legacy_boxscore = data.get('boxscore', {})
                teams = legacy_boxscore.get('teams', [])
                for team in teams:
                    team_abbr_api = team.get('team', {}).get('abbreviation', '').lower()
                    # Allow alternate mappings (for example, for Brooklyn Nets)
                    if team_abbr_api == team_abbrev or (team_abbrev == 'bkn' and team_abbr_api in ['bkn', 'brooklyn']):
                        stats_list = team.get('statistics', [])
                        if stats_list:
                            stats = stats_list[0]  # assume the first group holds the combined stats
                            raw_stats = {
                                'fieldGoalsMade-fieldGoalsAttempted': self._extract_stat_from_list(
                                    stats, 'fieldGoalsMade-fieldGoalsAttempted'),
                                'fieldGoalPct': self._extract_stat_from_list(stats, 'fieldGoalPct'),
                                'threePointFieldGoalsMade-threePointFieldGoalsAttempted': self._extract_stat_from_list(
                                    stats, 'threePointFieldGoalsMade-threePointFieldGoalsAttempted'),
                                'threePointFieldGoalPct': self._extract_stat_from_list(stats, 'threePointFieldGoalPct'),
                                'freeThrowsMade-freeThrowsAttempted': self._extract_stat_from_list(
                                    stats, 'freeThrowsMade-freeThrowsAttempted'),
                                'freeThrowPct': self._extract_stat_from_list(stats, 'freeThrowPct'),
                                'offensiveRebounds': self._extract_stat_from_list(stats, 'offensiveRebounds'),
                                'defensiveRebounds': self._extract_stat_from_list(stats, 'defensiveRebounds'),
                                'assists': self._extract_stat_from_list(stats, 'assists'),
                                'steals': self._extract_stat_from_list(stats, 'steals'),
                                'blocks': self._extract_stat_from_list(stats, 'blocks'),
                                'turnovers': self._extract_stat_from_list(stats, 'turnovers'),
                            }
                            # Calculate points as:
                            # points = free throws + (three-pointers * 3) + (two-pointers * 2)
                            fg_made = int(raw_stats.get('fieldGoalsMade-fieldGoalsAttempted', '0-0').split('-')[0] or "0")
                            tp_made = int(raw_stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted', '0-0').split('-')[0] or "0")
                            ft_made = int(raw_stats.get('freeThrowsMade-freeThrowsAttempted', '0-0').split('-')[0] or "0")
                            points = ft_made + (tp_made * 3) + ((fg_made - tp_made) * 2)
                            raw_stats['points'] = str(points)
                            break
                if not raw_stats:
                    logging.error(f"No raw stats found for team {team_abbrev} in legacy extraction.")
                    return {}
            else:
                # For new structure, if points not already calculated, compute them.
                if 'points' not in raw_stats:
                    fg_made = int(raw_stats.get('fieldGoalsMade-fieldGoalsAttempted', '0-0').split('-')[0] or "0")
                    tp_made = int(raw_stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted', '0-0').split('-')[0] or "0")
                    ft_made = int(raw_stats.get('freeThrowsMade-freeThrowsAttempted', '0-0').split('-')[0] or "0")
                    points = ft_made + (tp_made * 3) + ((fg_made - tp_made) * 2)
                    raw_stats['points'] = str(points)
            team_stats[team_abbrev] = raw_stats
            logging.info(f"Successfully extracted stats for team {team_abbrev}")
        except Exception as e:
            logging.error(f"Error extracting team stats: {str(e)}")
        return team_stats

    def extract_player_stats(self, data: dict) -> Dict[str, List[Dict]]:
        """
        Extract player statistics.
        First, try the new structure (player_stats array with metadata);
        if not present, fall back to legacy extraction from the "boxscore" key.
        """
        player_stats = defaultdict(list)
        try:
            players_data = data.get('player_stats', [])
            if not players_data:
                logging.warning("No 'player_stats' found in data; attempting legacy extraction from 'boxscore'.")
                legacy_boxscore = data.get('boxscore', {})
                players_teams = legacy_boxscore.get('players', [])
                for team in players_teams:
                    team_abbrev = team.get('team', {}).get('abbreviation', '').lower()
                    stat_groups = team.get('statistics', [])
                    if not stat_groups:
                        continue
                    athletes = stat_groups[0].get('athletes', [])
                    for athlete in athletes:
                        if athlete.get('didNotPlay'):
                            continue
                        player_dict = {
                            'name': athlete.get('athlete', {}).get('displayName', ''),
                            'position': athlete.get('athlete', {}).get('position', {}).get('abbreviation', ''),
                            'starter': athlete.get('starter', False)
                        }
                        raw_stats = athlete.get('stats', [])
                        # Attempt to convert minutes from "MM:SS" if necessary
                        try:
                            minutes_str = raw_stats[0]
                            if ':' in minutes_str:
                                parts = minutes_str.split(':')
                                minutes = float(parts[0]) + float(parts[1]) / 60
                            else:
                                minutes = float(minutes_str)
                        except Exception:
                            minutes = 0.0
                        processed_stats = {
                            'minutes': str(minutes),
                            'points': raw_stats[13] if len(raw_stats) > 13 else "0",
                            'rebounds': raw_stats[6] if len(raw_stats) > 6 else "0",
                            'assists': raw_stats[7] if len(raw_stats) > 7 else "0",
                            'steals': raw_stats[8] if len(raw_stats) > 8 else "0",
                            'blocks': raw_stats[9] if len(raw_stats) > 9 else "0",
                            'turnovers': raw_stats[10] if len(raw_stats) > 10 else "0",
                            'fieldGoalsMade': raw_stats[1].split('-')[0] if len(raw_stats) > 1 and '-' in raw_stats[1] else "0",
                            'fieldGoalsAttempted': raw_stats[1].split('-')[1] if len(raw_stats) > 1 and '-' in raw_stats[1] else "0",
                            'threePointFieldGoalsMade': raw_stats[2].split('-')[0] if len(raw_stats) > 2 and '-' in raw_stats[2] else "0",
                            'threePointFieldGoalsAttempted': raw_stats[2].split('-')[1] if len(raw_stats) > 2 and '-' in raw_stats[2] else "0",
                        }
                        player_dict['stats'] = processed_stats
                        player_stats[team_abbrev].append(player_dict)
            else:
                # New structure extraction (assumes data contains 'metadata' and a player_stats array)
                team_abbrev = data.get('metadata', {}).get('team', '').lower()
                if team_abbrev == "gs":
                    team_abbrev = "gsw"
                for player in players_data:
                    try:
                        player_dict = {
                            'name': player.get('name', ''),
                            'position': player.get('position', ''),
                            'starter': player.get('starter', False),
                        }
                        stats = player.get('stats', {})
                        processed_stats = {
                            'minutes': stats.get('minutes', '0'),
                            'points': stats.get('points', '0'),
                            'rebounds': stats.get('rebounds', '0'),
                            'assists': stats.get('assists', '0'),
                            'steals': stats.get('steals', '0'),
                            'blocks': stats.get('blocks', '0'),
                            'turnovers': stats.get('turnovers', '0'),
                            'fieldGoalsMade': stats.get('fieldGoalsMade-fieldGoalsAttempted', '0-0').split('-')[0],
                            'fieldGoalsAttempted': (stats.get('fieldGoalsMade-fieldGoalsAttempted', '0-0').split('-')[1]
                                                      if '-' in stats.get('fieldGoalsMade-fieldGoalsAttempted', '0-0') else "0"),
                            'threePointFieldGoalsMade': stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted', '0-0').split('-')[0],
                            'threePointFieldGoalsAttempted': (stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted', '0-0').split('-')[1]
                                                               if '-' in stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted', '0-0') else "0"),
                        }
                        player_dict['stats'] = processed_stats
                        player_stats[team_abbrev].append(player_dict)
                    except Exception as e:
                        logging.error(f"Error processing player {player.get('name', 'unknown')}: {str(e)}")
                        continue
                logging.info("Successfully extracted player stats from new structure.")
        except Exception as e:
            logging.error(f"Error extracting player stats: {str(e)}")
        return dict(player_stats)

    def calculate_shooting_stats(self, stats: Dict[str, str]) -> Dict[str, float]:
        """Calculate shooting statistics"""
        try:
            # Field Goals
            fg = stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')
            fgm, fga = int(fg[0]), int(fg[1])
            # Three Pointers
            tp = stats['threePointFieldGoalsMade-threePointFieldGoalsAttempted'].split('-')
            tpm, tpa = int(tp[0]), int(tp[1])
            # Calculate eFG%
            efg = (fgm + 0.5 * tpm) / fga if fga > 0 else 0.0
            return {
                'efg': efg,
                'fg_pct': float(stats['fieldGoalPct']) / 100 if 'fieldGoalPct' in stats else 0.0,
                'tp_pct': float(stats['threePointFieldGoalPct']) / 100 if 'threePointFieldGoalPct' in stats else 0.0,
                'fgm': fgm,
                'fga': fga,
                'tpm': tpm,
                'tpa': tpa
            }
        except (KeyError, ValueError) as e:
            print(f"Error calculating shooting stats: {e}")
            return {}

    def calculate_possession_stats(self, stats: Dict[str, str]) -> Dict[str, float]:
        """Calculate possession-based statistics"""
        try:
            fg = stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')
            fga = int(fg[1])
            ft = stats['freeThrowsMade-freeThrowsAttempted'].split('-')
            fta = int(ft[1])
            orb = int(stats['offensiveRebounds'])
            turnovers = int(stats['turnovers'])
            possessions = fga - orb + turnovers + (0.44 * fta)
            return {
                'possessions': possessions,
                'turnovers': turnovers,
                'offensive_rebounds': orb,
                'field_goal_attempts': fga,
                'free_throw_attempts': fta
            }
        except (KeyError, ValueError) as e:
            print(f"Error calculating possession stats: {e}")
            return {}

    def calculate_metrics_hash(self, basic_stats, shooting_stats, possession_stats):
        """Calculates a hash of the combined metrics for integrity checks."""
        combined_stats = {**basic_stats, **shooting_stats, **possession_stats}
        sorted_stats_str = json.dumps(combined_stats, sort_keys=True)
        return hashlib.sha256(sorted_stats_str.encode()).hexdigest()

    def calculate_player_metrics_hash(self, player_stats, stat_keys):
        """Calculates a hash of the player's statistics for integrity checks."""
        player_stats_dict = dict(zip(stat_keys, player_stats))
        sorted_stats_str = json.dumps(player_stats_dict, sort_keys=True)
        return hashlib.sha256(sorted_stats_str.encode()).hexdigest()

    def process_boxscore(self, event_id: str, team_abbr: str, game_data: dict) -> None:
        """Process a single boxscore file (using either new or legacy structure)"""
        logging.info(f"Processing boxscore for event {event_id}, team {team_abbr}")
        conn = self.get_db_connection()
        if not conn:
            logging.error("Could not establish database connection")
            return
        try:
            with conn:  # use transaction
                with conn.cursor() as cursor:
                    # Get game_id from the games table
                    cursor.execute(
                        "SELECT game_id FROM games WHERE event_id = %s",
                        (event_id,)
                    )
                    result = cursor.fetchone()
                    if not result:
                        logging.error(f"No matching game found for event_id: {event_id}")
                        return
                    game_id = result[0]

                    # Extract team stats (from new or legacy structure)
                    team_stats = self.extract_team_stats(game_data)
                    if not team_stats:
                        logging.error(f"No team stats extracted for game {event_id}")
                        return

                    # For each team extracted, calculate shooting and possession stats and insert
                    for team_abbrev_extracted, stats in team_stats.items():
                        shooting_stats = game_data.get('team_stats', {}).get('shooting', {})
                        possession_stats = game_data.get('team_stats', {}).get('possession', {})
                        team_stats_data = (
                            game_id,
                            team_abbrev_extracted,
                            int(stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')[0]),
                            int(stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')[1]),
                            int(stats['threePointFieldGoalsMade-threePointFieldGoalsAttempted'].split('-')[0]),
                            int(stats['threePointFieldGoalsMade-threePointFieldGoalsAttempted'].split('-')[1]),
                            int(stats['freeThrowsMade-freeThrowsAttempted'].split('-')[0]),
                            int(stats['freeThrowsMade-freeThrowsAttempted'].split('-')[1]),
                            int(stats['offensiveRebounds']),
                            int(stats['defensiveRebounds']),
                            int(stats['assists']),
                            int(stats['steals']),
                            int(stats['blocks']),
                            int(stats['turnovers']),
                            int(stats['points']),
                            float(shooting_stats.get('efg', 0.0)),
                            float(possession_stats.get('possessions', 0.0)),
                            self.calculate_metrics_hash(stats, shooting_stats, possession_stats)
                        )
                        cursor.execute("""
                            INSERT INTO team_statistics (
                                game_id, team_id, field_goals_made, field_goals_attempted,
                                three_pointers_made, three_pointers_attempted,
                                free_throws_made, free_throws_attempted,
                                offensive_rebounds, defensive_rebounds,
                                assists, steals, blocks, turnovers, points,
                                efficiency_rating, possession_count, metrics_hash
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, team_stats_data)

                    # Extract player stats (new or legacy)
                    player_stats = self.extract_player_stats(game_data)
                    for team_abbrev_player, players in player_stats.items():
                        for player_data in players:
                            # Convert minutes (handle "MM:SS" if necessary)
                            minutes_str = player_data['stats']['minutes']
                            try:
                                if ':' in minutes_str:
                                    parts = minutes_str.split(':')
                                    minutes = float(parts[0]) + float(parts[1]) / 60
                                else:
                                    minutes = float(minutes_str)
                            except Exception:
                                minutes = 0.0
                            player_stats_data = (
                                game_id,
                                team_abbrev_player,
                                player_data['name'],
                                round(minutes, 1),
                                int(player_data['stats']['points']),
                                int(player_data['stats']['rebounds']),
                                int(player_data['stats']['assists']),
                                int(player_data['stats']['steals']),
                                int(player_data['stats']['blocks']),
                                int(player_data['stats']['turnovers']),
                                int(player_data['stats']['fieldGoalsMade']),
                                int(player_data['stats']['fieldGoalsAttempted']),
                                int(player_data['stats']['threePointFieldGoalsMade']),
                                int(player_data['stats']['threePointFieldGoalsAttempted']),
                                self.calculate_player_metrics_hash(
                                    list(player_data['stats'].values()),
                                    list(player_data['stats'].keys())
                                )
                            )
                            cursor.execute("""
                                INSERT INTO player_statistics (
                                    game_id, team_id, player_name, minutes_played, points, rebounds,
                                    assists, steals, blocks, turnovers, field_goals_made,
                                    field_goals_attempted, three_pointers_made, three_pointers_attempted,
                                    metrics_hash
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, player_stats_data)
                    conn.commit()
                    logging.info(f"Successfully processed boxscore for event {event_id}")
        except Exception as e:
            logging.error(f"Error processing boxscore: {str(e)}")
            conn.rollback()
        finally:
            conn.close()

    def process_all_boxscores(self):
        """Process all boxscore files in the game_data directory"""
        files_processed = 0
        files_failed = 0

        print("\nProcessing boxscore files...")

        # Iterate through team folders in game_data
        for team_folder in os.listdir(self.game_data_path):
            team_path = os.path.join(self.game_data_path, team_folder)
            if os.path.isdir(team_path):
                # Iterate through boxscore files in each team folder
                for file_name in os.listdir(team_path):
                    if file_name.startswith("boxscore_") and file_name.endswith(".json"):
                        file_path = os.path.join(team_path, file_name)
                        try:
                            with open(file_path, 'r') as f:
                                game_data = json.load(f)
                            # Extract event ID and team abbreviation from the file/folder names
                            event_id = file_name.split('_')[1].split('.')[0]
                            team_abbrev = team_folder
                            self.process_boxscore(event_id, team_abbrev, game_data)
                            files_processed += 1
                            if files_processed % 10 == 0:
                                print(f"Processed {files_processed} files...")
                        except Exception as e:
                            print(f"Failed to process {file_name}: {e}")
                            files_failed += 1

        print(f"\nProcessing complete:")
        print(f"Successfully processed: {files_processed} files")
        print(f"Failed: {files_failed} files")
