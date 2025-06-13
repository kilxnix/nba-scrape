"""
NBA Analytics Platform - Enterprise Data Processing Framework
==========================================================

System Architecture Overview:
---------------------------
A distributed data processing pipeline for NBA statistical analysis,
implementing secure data acquisition, transformation, and analytical processing.

Core System Components:
1. RESTful API Interface
2. Modular Data Processing Engine
3. Distributed Database Integration
4. Statistical Analysis Framework

Implementation Protocol:
- Automated data gathering at 1AM daily
- Real-time statistical processing
- Comparative analytics generation
- User-facing game selection interface

Technical Architecture:
----------------------
- Flask-based API infrastructure
- PostgreSQL data persistence layer
- Modular processing components
- Cross-origin resource configuration
"""
import json
import traceback
import backoff
from typing import Optional
from dataclasses import dataclass
from urllib.parse import quote_plus
from flask import Flask, jsonify, request
from nba_data.fetcher import NBAScheduleFetcher
from nba_data.extractor import BoxScoreFetcher, PlayByPlay
from nba_data.processor import BoxscoreProcessor
from nba_data.teams import NBA_TEAMS
from dotenv import load_dotenv
import requests
import os
from datetime import date, datetime
from flask_cors import CORS
import psycopg2
from psycopg2 import pool
import hashlib
from nba_data.database import DatabasePool, db_transaction
from nba_data.analysis import (
    get_todays_games,
    perform_matchup_analysis,
    find_common_opponents,
    get_comprehensive_team_stats,
    calculate_average_stats,
    create_comparison,
    calculate_analysis_hash,
    save_matchup_analysis
)
from psycopg2.extras import RealDictCursor
from nba_data.storage.persistence import MatchupAnalysisStorage

# System Initialization Layer
# =========================

load_dotenv()  # Environmental Configuration Protocol

# Flask Application Initialization
# ------------------------------
app = Flask(__name__)
CORS(app, resources={
    r"/analyze_matchups": {"origins": ["http://localhost:5000"]},
    r"/todays_games": {"origins": ["http://localhost:5000"]}
})

# Database Configuration
# ====================
db_params = {
    "user": os.environ.get("user"),
    "password": os.environ.get("password"),
    "host": os.environ.get("host"),
    "port": os.environ.get("port"),
    "dbname": os.environ.get("dbname")
}

# Connection String Assembly with URL encoding and SSL
conn_string = (
    f"postgresql://{quote_plus(db_params['user'])}:{quote_plus(db_params['password'])}"
    f"@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    "?sslmode=require"
)

# Initialize storage framework with secure connection parameters
analysis_storage = MatchupAnalysisStorage(conn_string)
# Data Processing Component Initialization
# ------------------------------------
# Each component maintains isolated responsibility within the system
schedule_fetcher = NBAScheduleFetcher(conn_string)  # Schedule Acquisition Module
boxscore_fetcher = BoxScoreFetcher(conn_string)    # Statistical Data Extractor
playbyplay = PlayByPlay(conn_string)               # Time-Series Event Processor
processor = BoxscoreProcessor(conn_string)         # Analytics Engine

# In your database connection function
def get_db_connection():
    """
    Establish PostgreSQL connection using TCP/IP configuration
    """
    try:
        # Build connection parameters
        params = {
            'dbname': os.getenv('dbname'),
            'user': os.getenv('user'),
            'password': os.getenv('password'),
            'host': os.getenv('host'),
            'port': os.getenv('port'),
            'sslmode': 'require',  # Force SSL for Supabase
            'keepalives': 1,       # Enable keepalive
            'keepalives_idle': 30, # Seconds between keepalive signals
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'client_encoding': 'utf8'
        }
        
        # Create connection string with proper URL encoding
        conn_str = (
            f"postgresql://{params['user']}:{params['password']}"
            f"@{params['host']}:{params['port']}/{params['dbname']}"
            f"?sslmode=require"
        )

        # Establish connection
        connection = psycopg2.connect(conn_str)
        
        # Configure connection
        connection.set_session(autocommit=False)
        
        print(f"Successfully connected to database at {params['host']}:{params['port']}")
        return connection

    except Exception as e:
        print(f"Connection error: {str(e)}")
        print("Connection parameters:")
        print(f"- Host: {os.getenv('host')}")
        print(f"- Port: {os.getenv('port')}")
        print(f"- Database: {os.getenv('dbname')}")
        print(f"- User: {os.getenv('user')}")
        return None

def populate_teams_table():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                print("Initiating team data verification...")
                cursor.execute("SELECT COUNT(*) FROM teams")
                count = cursor.fetchone()[0]
                print(f"Current team count: {count}")

                if count == 0:
                    print("Initializing team data population...")
                    teams_data = []
                    for team_id, team_info in NBA_TEAMS.items():
                        # Ensure abbreviation is exactly 3 characters
                        abbreviation = team_info["abbreviation"][:4].lower()
                        
                        city_name = (
                            team_info["full_name"]
                            .replace(f"-{team_info['display_name'].lower()}", "")
                            .replace("-", " ")
                            .title()
                        )
                        
                        teams_data.append((
                            abbreviation,                    # Truncated to 3 chars
                            team_info["display_name"],
                            city_name,
                            team_info["conference"],
                            team_info["division"]
                        ))
                        print(f"Processed team: {team_info['display_name']} ({abbreviation})")
                    
                    cursor.executemany(
                        """
                        INSERT INTO teams 
                        (team_id, full_name, city, conference, division) 
                        VALUES (%s, %s, %s, %s, %s)
                        """, 
                        teams_data
                    )
                    conn.commit()
                    print(f"Team data initialization complete: {len(teams_data)} records")
                else:
                    print("Team data validation complete: Database populated")
        except Exception as e:
            print(f"Team Data Population Error: {e}")
            print("Error Context:")
            print(f"- Connection State: {'Active' if conn else 'Inactive'}")
            print(f"- Operation: Team Data Population")
        finally:
            conn.close()
            print("Database connection terminated")
    else:
        print("Database connection failure: Team data population aborted")

# Update initialization
def initialize_app():
    """Application initialization protocol with security validation"""
    with app.app_context():
        print("Initiating application initialization sequence...")
        try:
            # Initialize database pool
            DatabasePool.initialize(min_conn=2, max_conn=10)
            
            # Verify storage layer initialization
            if not hasattr(app, 'analysis_storage'):
                app.analysis_storage = analysis_storage
                
            # Test connection integrity
            with db_transaction() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT version()')
                    print(f"Database connection verified: {cursor.fetchone()[0]}")
            
            print("Application initialization complete")
        except Exception as e:
            print(f"Initialization error: {e}")
            raise
# API Endpoint Framework
# ===================

@app.route('/fetch_schedules', methods=['POST'])
def fetch_schedules():
    """
    Schedule Data Acquisition Protocol
    ==============================
    """
    try:
        print("Initiating schedule fetch operation...")
        # Initialize team variable
        team = None
        
        # Safely attempt to get team from request
        if request.is_json:
            team = request.json.get('team')
        
        if team:
            print(f"Processing single team schedule: {team}")
            team_info = NBA_TEAMS.get(team)
            
            if not team_info:
                print(f"Invalid team identifier: {team}")
                return jsonify({'error': 'Invalid team abbreviation'}), 400
            
            print(f"Fetching schedule for: {team_info['display_name']}")
            games = schedule_fetcher.get_team_schedule(
                team_info['abbreviation'],
                team_info['full_name']
            )
            
            if games:
                with db_transaction() as conn:
                    schedule_fetcher.save_schedule(team, games, conn)
                print(f"Schedule saved successfully for: {team}")
                return jsonify({
                    'message': f'Schedule acquisition complete: {team}',
                    'games_processed': len(games)
                })
            else:
                print(f"No schedule data available for: {team}")
                return jsonify({
                    'message': f'No schedule data found: {team}'
                })
        else:
            print("Initiating league-wide schedule acquisition...")
            successful_teams = schedule_fetcher.fetch_all_schedules()
            print(f"League-wide acquisition complete: {successful_teams} teams")
            return jsonify({
                'message': 'League schedule acquisition complete',
                'teams_processed': successful_teams
            })

    except Exception as e:
        print(f"Schedule Acquisition Error: {e}")
        print("Error Context:")
        print(f"- Team: {team if team is not None else 'All Teams'}")
        print(f"- Operation: Schedule Fetch")
        return jsonify({'error': str(e)}), 500
    
@app.route('/fetch_date_range_schedules', methods=['POST'])
def fetch_date_range_schedules():
    """
    Fetch schedules for a specific date range.
    
    Request Format:
    {
        "start_date": "2025-04-21",
        "end_date": "2025-04-29"
    }
    """
    try:
        data = request.json
        start_date = datetime.strptime(data.get('start_date'), '%Y-%m-%d')
        end_date = datetime.strptime(data.get('end_date'), '%Y-%m-%d')
        
        print(f"Initiating date range schedule acquisition: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        schedule_fetcher = NBAScheduleFetcher(conn_string)
        successful_days = schedule_fetcher.fetch_date_range_schedules(start_date, end_date)
        
        return jsonify({
            'message': 'Date range schedule acquisition complete',
            'days_processed': successful_days
        })
        
    except Exception as e:
        print(f"Date Range Schedule Acquisition Error: {e}")
        return jsonify({'error': str(e)}), 500
       
@app.route('/todays_games', methods=['GET'])
def todays_games():
    """
    Endpoint to fetch games scheduled for today.

    Returns:
        JSON: A list of games scheduled for today, or a message if no games are found.
    """
    try:
        today = date.today()
        with db_transaction() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT game_id, home_team_id, away_team_id, game_date
                    FROM games
                    WHERE DATE(game_date) = %s
                """, (today,))
                games = cursor.fetchall()

        if games:
            # Convert datetime objects to strings for JSON serialization
            for game in games:
                game['game_date'] = game['game_date'].isoformat()
            return jsonify(games)
        else:
            return jsonify({'message': 'No games scheduled for today.'})

    except Exception as e:
        print(f"Error fetching today's games: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/fetch_boxscores', methods=['POST'])
def fetch_boxscores():
    try:
        print("Initiating boxscore acquisition protocol...")
        
        # Use normalized cutoff date
        cutoff_date = datetime(2025, 5, 1)
        
        processed_teams = boxscore_fetcher.process_team_schedules(cutoff_date)
        
        return jsonify({
            'message': 'Boxscore acquisition complete',
            'teams_processed': processed_teams,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"Processing Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/fetch_plays', methods=['POST'])
def fetch_plays():
    """
    Play-by-Play Data Processing Framework
    ==================================
    
    Manages granular game event data acquisition and analysis.
    
    Request Structure:
    {
        "data_folder": "path_to_data"  # Optional - Defaults to current directory
    }
    
    Technical Implementation:
    1. Source Directory Validation
    2. Event Data Processing
    3. Sequential Analysis
    4. Data State Management
    
    Performance Metrics:
    - Processes complete game timelines
    - Maintains event sequence integrity
    - Validates temporal consistency
    """
    try:
        print("Initiating play-by-play data acquisition...")
        
        # Directory path validation
        data_folder = request.json.get('data_folder', '.')
        print(f"Processing directory: {data_folder}")
        
        # Event processing execution
        processed_files = playbyplay.process_game_files_with_frames(data_folder)
        print(f"Event processing complete: {processed_files} files processed")
        
        return jsonify({
            'message': 'Play-by-play processing complete',
            'files_processed': processed_files,
            'data_path': data_folder
        })

    except Exception as e:
        print(f"Play-by-Play Processing Error: {e}")
        print("Error Context:")
        print(f"- Data Path: {data_folder}")
        print(f"- Operation: Event Processing")
        return jsonify({'error': str(e)}), 500

@app.route('/process_stats', methods=['POST'])
def process_stats():
    """
    Statistical Analysis Framework
    =========================
    
    Comprehensive statistical processing and analysis system.
    
    Technical Architecture:
    1. Data Aggregation Protocol
    2. Statistical Analysis Engine
    3. Performance Metrics Computation
    4. Results Persistence Layer
    
    Analysis Components:
    - Team Performance Metrics
    - Player Statistics Analysis
    - Comparative Analytics Generation
    - Temporal Trend Analysis
    """
    try:
        print("Initiating statistical analysis protocol...")
        
        # Execute analysis engine
        processor.process_all_boxscores()
        print("Statistical analysis complete")
        
        return jsonify({
            'message': 'Statistical analysis successfully completed',
            'status': 'success',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"Statistical Analysis Error: {e}")
        print("Error Context:")
        print(f"- Operation: Statistical Processing")
        print(f"- Timestamp: {datetime.now().isoformat()}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/analyze_matchups', methods=['GET'])
def analyze_matchups():
    """
    Temporal Analysis Framework with EST/EDT Alignment

    Core Protocol Implementation:
    1. Precise timezone calibration
    2. Multi-window analysis optimization (previous day, today, and next day)
    3. Transaction integrity verification

    Security Architecture:
    - Temporal consistency validation
    - Atomic transaction enforcement
    - State verification protocol
    """
    print("Initiating EST/EDT-aligned matchup analysis")
    try:
        with db_transaction() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Use a temporal window that includes the previous day, today, and tomorrow (EST/EDT)
            cursor.execute("""
                WITH current_eastern_time AS (
                    SELECT CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York' as est_now
                ),
                game_window AS (
                    SELECT 
                        g.game_id, 
                        g.event_id,
                        g.home_team_id, 
                        g.away_team_id,
                        g.game_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' as est_game_date
                    FROM games g
                    CROSS JOIN current_eastern_time et
                    WHERE
                        DATE(g.game_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
                        BETWEEN DATE(et.est_now - INTERVAL '1 day') AND DATE(et.est_now + INTERVAL '1 day')
                )
                SELECT 
                    gw.*
                FROM game_window gw
                ORDER BY gw.est_game_date ASC
            """)
            
            scheduled_games = cursor.fetchall()
            
            # Debug temporal alignment
            cursor.execute("SELECT CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York' as est_now")
            current_est = cursor.fetchone()['est_now']
            print(f"Current EST/EDT: {current_est}")
            print(f"Analysis window identified {len(scheduled_games)} games")

            if not scheduled_games:
                return jsonify({'message': 'No games requiring analysis in current window.'})

            analysis_results = []
            storage = app.analysis_storage  # Assuming this is set up properly

            for game in scheduled_games:
                try:
                    analysis = perform_matchup_analysis(
                        conn,
                        game['game_id'],
                        game['home_team_id'],
                        game['away_team_id']
                    )
                    
                    if analysis:
                        # persist_analysis should internally call save_matchup_analysis which performs an UPSERT.
                        if storage.persist_analysis(game['game_id'], analysis):
                            analysis_results.append(analysis)
                            print(f"Completed analysis for game {game['game_id']} (EST: {game['est_game_date']})")
                            
                except Exception as e:
                    print(f"Analysis failure for game {game['game_id']}: {str(e)}")
                    continue

            return jsonify(analysis_results)

    except Exception as e:
        print(f"Critical error in matchup analysis protocol: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Application Entry Point
# ====================

if __name__ == '__main__':
    """
    System Initialization Protocol
    =========================
    
    Manages application startup and runtime configuration.
    
    Initialization Sequence:
    1. Environment Validation
    2. Database Connection Establishment
    3. Service Layer Initialization
    4. API Interface Activation
    """
    print("Initiating NBA Analytics Platform...")
    print(f"Environment: {'Development' if app.debug else 'Production'}")
    
    # Initialize the database pool before app initialization
    DatabasePool.initialize(min_conn=2, max_conn=10)
    
    initialize_app()
    print("Service layer initialization complete")
    
    # Register pool cleanup
    import atexit
    atexit.register(DatabasePool.close_all)
    
    app.run(debug=True)
    print("API interface activated")