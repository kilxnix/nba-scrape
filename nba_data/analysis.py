# nba_data/analysis.py
import json
import hashlib
from collections import defaultdict
from datetime import date, datetime, timedelta
import traceback
from psycopg2.extras import RealDictCursor
from nba_data.database import db_transaction

def get_todays_games():
    """
    Fetches games scheduled for the previous day, today, and the next day.

    Returns:
        List[Dict]: A list of dictionaries, each representing a game.
    """
    today = date.today()
    prev_day = today - timedelta(days=1)
    next_day = today + timedelta(days=1)
    try:
        with db_transaction() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT game_id, home_team_id, away_team_id, game_date
                    FROM games
                    WHERE DATE(game_date) BETWEEN %s AND %s
                """, (prev_day, next_day))
                games = cursor.fetchall()

                # Convert datetime objects to strings (for JSON serialization)
                for game in games:
                    for key, value in game.items():
                        if isinstance(value, (datetime, date)):
                            game[key] = value.isoformat()

                return games
    except Exception as e:
        print(f"Error fetching games for three day window: {e}")
        return []
    
def perform_matchup_analysis(conn, game_id, team1_id, team2_id):
    """
    Performs complete statistical analysis including player performance metrics.
    """
    try:
        # Find common opponents
        common_opponents = find_common_opponents(conn, team1_id, team2_id)
        
        # Get complete team statistics including player performances
        team1_stats = get_comprehensive_team_stats(conn, team1_id, common_opponents)
        team2_stats = get_comprehensive_team_stats(conn, team2_id, common_opponents)

        # Calculate advanced metrics
        team1_avg = calculate_advanced_stats(team1_stats)
        team2_avg = calculate_advanced_stats(team2_stats)

        # Create detailed comparison
        comparison = create_comparison(team1_avg, team2_avg)

        return {
            'game_id': game_id,
            'team1': team1_id,
            'team2': team2_id,
            'common_opponents': common_opponents,
            'team1_avg_stats': team1_avg,
            'team2_avg_stats': team2_avg,
            'comparison': comparison
        }

    except Exception as e:
        print(f"Error in comprehensive analysis: {str(e)}")
        traceback.print_exc()
        return None

def find_common_opponents(conn, team1_id, team2_id):
    """
    Finds common opponents that both teams have played against.

    Args:
        conn: Database connection object.
        team1_id (str): The ID of the first team.
        team2_id (str): The ID of the second team.

    Returns:
        List[str]: A list of common opponent team IDs.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT a.opponent_team_id
            FROM (
                SELECT CASE WHEN home_team_id = %s THEN away_team_id ELSE home_team_id END as opponent_team_id
                FROM games
                WHERE home_team_id = %s OR away_team_id = %s
            ) AS a
            INNER JOIN (
                SELECT CASE WHEN home_team_id = %s THEN away_team_id ELSE home_team_id END as opponent_team_id
                FROM games
                WHERE home_team_id = %s OR away_team_id = %s
            ) AS b ON a.opponent_team_id = b.opponent_team_id
            GROUP BY a.opponent_team_id
        """, (team1_id, team1_id, team1_id, team2_id, team2_id, team2_id))
        common_opponents = [row[0] for row in cursor.fetchall()]
    return common_opponents

def get_comprehensive_team_stats(conn, team_id, opponents):
    """
    Retrieves complete team and player statistics against common opponents.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            WITH team_games AS (
                SELECT DISTINCT g.game_id
                FROM games g
                WHERE (g.home_team_id = %s OR g.away_team_id = %s)
                AND (g.home_team_id = ANY(%s) OR g.away_team_id = ANY(%s))
            )
            SELECT 
                ts.*,
                json_agg(DISTINCT ps.*) as player_stats
            FROM team_games tg
            JOIN team_statistics ts ON tg.game_id = ts.game_id
            LEFT JOIN player_statistics ps ON tg.game_id = ps.game_id 
                AND ts.team_id = ps.team_id
            WHERE ts.team_id = %s
            GROUP BY ts.stat_id, ts.game_id, ts.team_id
        """, (team_id, team_id, opponents, opponents, team_id))
        
        return cursor.fetchall()

def calculate_average_stats(team_stats):
    """
    Calculates the average statistics for a team.

    Args:
        team_stats (List[Dict]): A list of dictionaries, each containing team stats for a game.

    Returns:
        Dict: A dictionary containing the average statistics.
    """
    num_games = len(team_stats)
    if num_games == 0:
        return {}

    # Initialize sums
    sum_stats = defaultdict(float)

    for game_stats in team_stats:
        for key, value in game_stats.items():
            if isinstance(value, (int, float)) and key != 'game_id':  # Exclude non-numeric and 'game_id'
                sum_stats[key] += value

    # Calculate averages
    avg_stats = {key: val / num_games for key, val in sum_stats.items()}
    return avg_stats

def calculate_advanced_stats(team_stats):
    """
    Calculates advanced statistical metrics including player contributions.
    """
    if not team_stats:
        return {}

    aggregated_stats = defaultdict(float)
    player_stats = defaultdict(lambda: defaultdict(float))
    games_count = len(team_stats)

    for game in team_stats:
        # Aggregate team stats
        for key, value in game.items():
            if isinstance(value, (int, float)) and key != 'game_id':
                aggregated_stats[key] += value

        # Aggregate player stats safely
        players = game.get('player_stats') or []  # Ensure we have a list, even if it's empty
        for player in players:
            if not player:  # Skip if the player record is None or falsy
                continue
            # Ensure the player has a 'player_name' key
            player_name = player.get('player_name')
            if not player_name:
                continue
            # Store player position (non-numeric field)
            if player.get('player_position'):
                player_stats[player_name]['position'] = player.get('player_position')
                
            for stat, value in player.items():
                if isinstance(value, (int, float)):
                    player_stats[player_name][stat] += value

    # Calculate averages
    avg_stats = {k: round(v / games_count, 2) for k, v in aggregated_stats.items()}
    avg_stats['player_averages'] = {}

    # Process player averages with position verification
    for player, stats in player_stats.items():
        player_avg = {}
        for stat, value in stats.items():
            if stat == 'position':
                if value == 'UNKNOWN':
                    print(f"⚠️ Found UNKNOWN position for player: {player}")
            player_avg[stat] = round(value / games_count, 2) if isinstance(value, (int, float)) else value
        avg_stats['player_averages'][player] = player_avg

    return avg_stats

def create_comparison(team1_avg, team2_avg):
    """
    Creates a side-by-side comparison of the two teams' average statistics.

    Args:
        team1_avg (Dict): Average statistics for team 1.
        team2_avg (Dict): Average statistics for team 2.

    Returns:
        Dict: A dictionary containing the comparative analysis.
    """
    comparison = {}
    all_keys = set(team1_avg.keys()).union(team2_avg.keys())

    for key in all_keys:
        comparison[key] = {
            'team1': team1_avg.get(key, 0),
            'team2': team2_avg.get(key, 0)
        }
    return comparison

def calculate_analysis_hash(comparative_analysis):
    """
    Calculates a SHA-256 hash of the comparative analysis data.

    Args:
        comparative_analysis (Dict): The comparative analysis data.

    Returns:
        str: The SHA-256 hash as a hexadecimal string.
    """
    # Convert the dictionary to a JSON string, sorting keys for consistency
    analysis_str = json.dumps(comparative_analysis, sort_keys=True)
    
    # Calculate the SHA-256 hash
    analysis_hash = hashlib.sha256(analysis_str.encode()).hexdigest()
    
    return analysis_hash

def save_matchup_analysis(conn, game_id: str, analysis_data: dict) -> None:
    """
    Saves matchup analysis results with proper referential integrity.
    
    Implementation:
    1. Extracts team IDs from analysis data
    2. Generates cryptographic hash for data verification
    3. Implements transactional integrity with conflict resolution
    4. Updates timestamps for audit trail
    
    Args:
        conn: Database connection object
        game_id: The game identifier
        analysis_data: Complete analysis data structure
    """
    try:
        with conn.cursor() as cursor:
            # Extract team IDs from analysis data
            home_team_id = analysis_data['team1']
            away_team_id = analysis_data['team2']
            
            # Generate analysis hash for data integrity
            analysis_hash = calculate_analysis_hash(analysis_data)
            
            # Perform upsert with full column specification
            cursor.execute("""
                INSERT INTO matchup_analysis (
                    game_id,
                    home_team_id,
                    away_team_id,
                    analysis_data,
                    analysis_hash,
                    created_at,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (game_id) 
                DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data,
                    analysis_hash = EXCLUDED.analysis_hash,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                game_id,
                home_team_id,
                away_team_id,
                json.dumps(analysis_data),
                analysis_hash
            ))

            # Update analysis queue status if exists
            cursor.execute("""
                UPDATE matchup_analysis_queue
                SET status = 'completed',
                    processed_at = CURRENT_TIMESTAMP
                WHERE game_id = %s
            """, (game_id,))
            
            conn.commit()
            print(f"Analysis saved successfully for game {game_id}")

    except Exception as e:
        print(f"Error saving matchup analysis: {e}")
        conn.rollback()
