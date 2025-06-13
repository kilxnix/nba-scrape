import logging
import os
import time
import json
from typing import Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from .teams import NBA_TEAMS
import psycopg2
import hashlib
from datetime import datetime, timedelta
from .database import db_transaction
import requests
from .teams import NBA_TEAMS
from .utils import parse_game_date, current_season_start_year

class NBAScheduleFetcher:
    def __init__(self, conn_string: str):
        """
        Initialize the schedule fetcher with logging and Selenium configuration.
        """
        self.NBA_TEAMS = NBA_TEAMS
        self.setup_logging()
        self.setup_driver()
        self.conn_string = conn_string
        self.base_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'outputs')
        os.makedirs(self.base_output_dir, exist_ok=True)

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

    def setup_driver(self):
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # Use local ChromeDriver instead of Remote
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logging.info('ChromeDriver initialized successfully')
        except Exception as e:
            logging.error(f'Failed to initialize ChromeDriver: {e}')
            raise
    def _resolve_la_team_by_event_id(self, event_id):
        """
        Enhanced LA team resolution with multiple fallback mechanisms.
        
        This method implements a multi-stage resolution process to correctly
        identify whether 'LA' refers to Lakers (LAL) or Clippers (LAC).
        
        Args:
            event_id (str): ESPN event identifier
            
        Returns:
            str: 'lal' or 'lac' based on resolution, or None if unresolved
        """
        try:
            # Stage 1: Try the ESPN summary API first (most reliable)
            summary_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
            print(f"Resolving LA team for event {event_id} via ESPN Summary API")
            
            response = requests.get(summary_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                teams = data.get('header', {}).get('competitions', [{}])[0].get('competitors', [])
                
                for team in teams:
                    team_data = team.get('team', {})
                    team_name = team_data.get('displayName', '').lower()
                    team_shortname = team_data.get('shortDisplayName', '').lower()
                    
                    if 'lakers' in team_name or 'lakers' in team_shortname:
                        print(f"Resolved LA team to Lakers (LAL) via summary API")
                        return 'lal'
                    elif 'clippers' in team_name or 'clippers' in team_shortname:
                        print(f"Resolved LA team to Clippers (LAC) via summary API")
                        return 'lac'
            
            # Stage 2: Try the ESPN teams API (provides more detail on team names)
            teams_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/matchup?event={event_id}"
            print(f"Trying teams API for LA resolution")
            
            teams_response = requests.get(teams_url, timeout=10)
            if teams_response.status_code == 200:
                teams_data = teams_response.json()
                teams = [teams_data.get('team', {}), teams_data.get('opponent', {})]
                
                for team in teams:
                    if not team:
                        continue
                        
                    team_name = team.get('displayName', '').lower()
                    team_location = team.get('location', '').lower()
                    
                    if 'lakers' in team_name or 'lakers' in team_location:
                        print(f"Resolved LA team to Lakers (LAL) via teams API")
                        return 'lal'
                    elif 'clippers' in team_name or 'clippers' in team_location:
                        print(f"Resolved LA team to Clippers (LAC) via teams API")
                        return 'lac'
            
            # Stage 3: Fallback to Selenium scraping
            game_url = f"https://www.espn.com/nba/game/_/gameId/{event_id}"
            print(f"Using Selenium scraping for LA resolution")
            
            self.driver.get(game_url)
            time.sleep(3)  # Allow page to load
            
            # Look for team name in various elements
            page_source = self.driver.page_source.lower()
            
            # Check for specific team mentions
            if 'los angeles lakers' in page_source or 'la lakers' in page_source:
                print(f"Resolved LA team to Lakers (LAL) via page content")
                return 'lal'
            elif 'los angeles clippers' in page_source or 'la clippers' in page_source:
                print(f"Resolved LA team to Clippers (LAC) via page content")
                return 'lac'
            
            # Stage 4: Last resort - check schedules and stats pages
            schedules_url = f"https://www.espn.com/nba/team/schedule/_/name/la/los-angeles-lakers"
            self.driver.get(schedules_url)
            time.sleep(2)
            
            # If we find our event ID on the Lakers schedule
            if event_id in self.driver.page_source:
                print(f"Found event {event_id} on Lakers schedule")
                return 'lal'
                
            clippers_url = f"https://www.espn.com/nba/team/schedule/_/name/lac/los-angeles-clippers"
            self.driver.get(clippers_url)
            time.sleep(2)
            
            if event_id in self.driver.page_source:
                print(f"Found event {event_id} on Clippers schedule")
                return 'lac'
                
            # If all methods fail, log and return None
            print(f"All LA resolution methods failed for event {event_id}")
            return None
            
        except Exception as e:
            print(f"Error in LA team resolution for event {event_id}: {str(e)}")
            logging.error(f"LA resolution error: {str(e)}")
            return None
    def _resolve_team_by_selenium(self, event_id):
        """
        Improved team resolution that accurately extracts team identifiers from ESPN.
        
        Parameters:
            event_id (str): ESPN event identifier
                
        Returns:
            tuple: (home_team_id, away_team_id) or (None, None) if resolution fails
        """
        try:
            # First try the ESPN API directly - more reliable than scraping
            api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
            
            print(f"Resolving teams for event {event_id} using ESPN API")
            response = requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                teams = data.get('header', {}).get('competitions', [{}])[0].get('competitors', [])
                
                if len(teams) >= 2:
                    # ESPN format: home team is typically listed first
                    home_team_data = teams[0].get('team', {})
                    away_team_data = teams[1].get('team', {})
                    
                    # Extract abbreviations and normalize them
                    home_abbr = home_team_data.get('abbreviation', '').lower()
                    away_abbr = away_team_data.get('abbreviation', '').lower()
                    
                    # Normalize ESPN abbreviations to match your database
                    home_team_id = self._normalize_espn_team_code(home_abbr)
                    away_team_id = self._normalize_espn_team_code(away_abbr)
                    
                    if home_team_id and away_team_id:
                        print(f"API resolution success: home={home_team_id}, away={away_team_id}")
                        return home_team_id, away_team_id
            
            # Fallback to Selenium if API fails
            game_url = f"https://www.espn.com/nba/game/_/gameId/{event_id}"
            print(f"API resolution failed, falling back to Selenium for event {event_id}")
            
            self.driver.get(game_url)
            time.sleep(3)  # DOM stabilization interval
            
            # Extract team codes from team headers in score display
            team_elements = self.driver.find_elements(By.CSS_SELECTOR, ".ScoreCell__Team abbr")
            if not team_elements or len(team_elements) < 2:
                # Try alternative selectors if the primary one fails
                team_elements = self.driver.find_elements(By.CSS_SELECTOR, ".TeamLinks__Link")
            
            team_codes = []
            for elem in team_elements:
                if elem.get_attribute("title"):
                    team_name = elem.get_attribute("title").strip()
                    for team_id, team_info in self.NBA_TEAMS.items():
                        if team_info['display_name'].lower() in team_name.lower():
                            team_codes.append(team_id)
                            break
                elif elem.text:
                    code = elem.text.strip().upper()
                    team_codes.append(self._normalize_espn_team_code(code.lower()))
            
            if len(team_codes) >= 2:
                home_team_id = team_codes[0]
                away_team_id = team_codes[1]
                print(f"Selenium resolution success: home={home_team_id}, away={away_team_id}")
                return home_team_id, away_team_id
            
            # Final fallback - try to extract from page title or other elements
            title = self.driver.title
            team_names = []
            
            for team_id, team_info in self.NBA_TEAMS.items():
                if team_info['display_name'].lower() in title.lower():
                    team_names.append(team_id)
                    
            if len(team_names) >= 2:
                print(f"Title resolution success: {team_names[0]}, {team_names[1]}")
                return team_names[0], team_names[1]
                
            print(f"All resolution methods failed for event {event_id}")
            return None, None
            
        except Exception as e:
            print(f"Team resolution error: {str(e)}")
            logging.error(f"Team resolution error for event {event_id}: {str(e)}")
            return None, None
    def _normalize_espn_team_code(self, espn_code):
        """
        Normalizes ESPN team codes to match database team_ids.
        
        Args:
            espn_code (str): Team code from ESPN
            
        Returns:
            str: Normalized team_id for database
        """
        # Convert to lowercase for consistency
        code = espn_code.lower()
        
        # Direct mapping for special cases
        code_mapping = {
            'ny': 'nyk',    # New York Knicks
            'gs': 'gsw',    # Golden State Warriors
            'sa': 'sas',    # San Antonio Spurs
            'nop': 'no',    # New Orleans Pelicans
            'wsh': 'wsh',   # Washington Wizards
            'bkn': 'bkn',   # Brooklyn Nets
        }
        
        # Special handling for LA teams
        if code == 'la':
            # We'll need event context to resolve this - return None
            # and handle in the calling method
            return None
            
        # Return mapped code or original if no mapping exists
        return code_mapping.get(code, code)
    def _parse_opponent_info(self, opponent_str):
        """
        Normalizes opponent strings to standardized team identifiers.
        
        Parameters:
            opponent_str (str): Opponent string from ESPN
            
        Returns:
            tuple: (team_id, is_away) with normalized team ID and boolean away status
        """
        try:
            # Determine if the game is away
            is_away = '@' in opponent_str.lower()
            
            # Clean and normalize the opponent string
            opponent_clean = (
                opponent_str.replace('@', '')
                .replace('vs', '')
                .replace('vs.', '')
                .replace('*', '')
                .strip()
                .lower()
            )
            
            print(f"Processing opponent: '{opponent_str}' → '{opponent_clean}'")
            
            # Special case handling for specific teams
            if opponent_clean == "ny":
                return "nyk", is_away
            elif opponent_clean == "gs":
                return "gsw", is_away
            elif opponent_clean == "sa":
                return "sas", is_away
            elif opponent_clean == "nola":
                return "no", is_away
                
            # Direct LA resolution deferred to event-based method
            if opponent_clean == "la":
                print(f"Found 'LA' opponent - will resolve with event context")
                return None, is_away
                
            # Match against NBA_TEAMS
            for team_id, team_info in self.NBA_TEAMS.items():
                # Look for exact matches
                if opponent_clean == team_info["abbreviation"].lower():
                    return team_id, is_away
                    
                # Check against alternate names and variations
                if "alt_name" in team_info and any(alt.lower() == opponent_clean for alt in team_info["alt_name"]):
                    return team_id, is_away
                    
                # Check city and display name
                if opponent_clean == team_info["display_name"].lower() or opponent_clean == team_info["city"].lower():
                    return team_id, is_away
                    
                # Check full name with spaces instead of hyphens
                if opponent_clean == team_info["full_name"].replace('-', ' ').lower():
                    return team_id, is_away
                    
            # If no match found, log the failure
            print(f"⚠️ Could not match opponent: '{opponent_str}' (cleaned: '{opponent_clean}')")
            return None, is_away
            
        except Exception as e:
            print(f"Error parsing opponent info: {e}")
            return None, False
    def fetch_all_schedules(self):
        """
        Fetches schedules for all NBA teams with connection pooling.
        
        Implementation Notes:
        - Uses context manager for automatic connection handling
        - Implements per-team error isolation
        - Maintains transaction integrity across team processing
        - Returns count of successfully processed teams
        """
        successful_teams = 0
        
        try:
            for team_abbr, team_info in NBA_TEAMS.items():
                try:
                    print(f"\nProcessing {team_info['display_name']}...")
                    games = self.get_team_schedule(
                        team_info['abbreviation'],
                        team_info['full_name']
                    )
                    
                    if games:
                        # Use db_transaction context manager
                        with db_transaction() as conn:
                            # Pass connection to save_schedule
                            self.save_schedule(team_abbr, games, conn)
                            successful_teams += 1
                            print(f"✓ Fetched {len(games)} games for {team_info['display_name']}")
                    else:
                        print(f"✗ No games found for {team_info['display_name']}")
                        
                    # Rate limiting delay between teams
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"✗ Error processing {team_info['display_name']}: {str(e)}")
                    logging.error(f"Failed to process team {team_abbr}: {e}")
                    continue
            
            return successful_teams
            
        except Exception as e:
            logging.error(f"Fatal error in fetch_all_schedules: {e}")
            return successful_teams

    def get_team_schedule(self, team_abbr, team_full_name):
        """
        Fetch complete schedule for a specific team using Selenium.

        Args:
            team_abbr (str): Team abbreviation (e.g., 'lal')
            team_full_name (str): Full team name (e.g., 'los-angeles-lakers')

        Returns:
            list: List of dictionaries containing game information and event IDs
        """
        url = f"https://www.espn.com/nba/team/schedule/_/name/{team_abbr}/{team_full_name}"
        logging.info(f"Fetching schedule for team: {team_full_name}")
        
        try:
            self.driver.get(url)
            time.sleep(5)  # Allow time for the page to load
            
            # Get the page source after JavaScript execution
            html_source = self.driver.page_source
            soup = BeautifulSoup(html_source, 'html.parser')
            
            # Find all game rows in the schedule table
            games = []
            game_rows = soup.find_all('tr', class_='Table__TR')
            
            for row in game_rows:
                try:
                    # Find game link containing event ID
                    game_link = row.find('a', href=lambda x: x and 'gameId' in x)
                    if game_link:
                        event_id = game_link['href'].split('gameId/')[1].split('/')[0]
                        date_cell = row.find('td', class_='Table__TD')
                        opponent_cell = row.find_all('td', class_='Table__TD')[1]
                        
                        games.append({
                            'date': date_cell.text.strip() if date_cell else '',
                            'opponent': opponent_cell.text.strip() if opponent_cell else '',
                            'event_id': event_id
                        })
                except Exception as e:
                    logging.error(f"Error processing game row: {e}")
                    continue
            
            logging.info(f"Found {len(games)} games for {team_full_name}")
            return games
            
        except Exception as e:
            logging.error(f"Error fetching schedule: {e}")
            return []

    def fetch_team_schedule_api(self, team_abbr: str):
        """Fetch a team's schedule using ESPN's public JSON API."""
        schedule = []
        today = datetime.utcnow()
        season_start = datetime(current_season_start_year(today), 10, 1)
        season_end = datetime(current_season_start_year(today) + 1, 7, 1)
        cur = season_start
        while cur <= season_end:
            url = (
                "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard"
                f"?dates={cur.strftime('%Y-%m-%d')}"
            )
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    cur += timedelta(days=1)
                    continue
                data = resp.json()
                for event in data.get('events', []):
                    eid = event.get('id')
                    comp = event.get('competitions', [{}])[0]
                    comps = comp.get('competitors', [])
                    if len(comps) < 2:
                        continue
                    home = comps[0]['team']['abbreviation'].lower()
                    away = comps[1]['team']['abbreviation'].lower()
                    if team_abbr in [home, away]:
                        schedule.append(
                            {
                                'event_id': eid,
                                'date': event.get('date'),
                                'home_team': home,
                                'away_team': away,
                            }
                        )
            except Exception:
                pass
            cur += timedelta(days=1)
        return schedule
    
    def get_db_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.conn_string)
        except Exception as e:
            print(f"Error connecting to the database: {e}")
        return conn
    def save_schedule(self, team_abbr, games, conn):
        """
        Enhanced schedule saving with proper team resolution.
        """
        saved_games = 0
        cursor = None
        
        try:
            cursor = conn.cursor()
            
            for game in games:
                try:
                    # Extract game metadata
                    event_id = game['event_id']
                    date_str = game['date'].strip()

                    # Parse game date using dynamic season logic
                    parsed_dt = parse_game_date(date_str)
                    if not parsed_dt:
                        continue
                    game_date = parsed_dt.strftime('%Y-%m-%d')
                    
                    # Use the improved DOM-based resolution for all teams
                    home_team_id, away_team_id = self._resolve_team_by_selenium(event_id)
                    
                    if home_team_id and away_team_id:
                        # Teams successfully resolved via DOM/API
                        print(f"Teams resolved: home={home_team_id}, away={away_team_id}")
                        
                        # Validate that our team is one of them
                        if team_abbr not in [home_team_id, away_team_id]:
                            print(f"Warning: Current team {team_abbr} not found in resolved teams")
                            
                            # Check if this is due to abbreviation differences
                            if team_abbr == 'nyk' and ('ny' in [home_team_id, away_team_id]):
                                # Replace 'ny' with 'nyk'
                                if home_team_id == 'ny':
                                    home_team_id = 'nyk'
                                else:
                                    away_team_id = 'nyk'
                            elif team_abbr == 'gsw' and ('gs' in [home_team_id, away_team_id]):
                                # Replace 'gs' with 'gsw'
                                if home_team_id == 'gs':
                                    home_team_id = 'gsw'
                                else:
                                    away_team_id = 'gsw'
                            elif team_abbr == 'sas' and ('sa' in [home_team_id, away_team_id]):
                                # Replace 'sa' with 'sas'
                                if home_team_id == 'sa':
                                    home_team_id = 'sas'
                                else:
                                    away_team_id = 'sas'
                            elif team_abbr in ['lal', 'lac'] and ('la' in [home_team_id, away_team_id]):
                                # Special handling for LA teams
                                la_team = self._resolve_la_team_by_event_id(event_id)
                                if la_team:
                                    if home_team_id == 'la':
                                        home_team_id = la_team
                                    else:
                                        away_team_id = la_team
                                else:
                                    # Fallback: if we're processing Lakers, assume it's them
                                    if team_abbr == 'lal':
                                        if home_team_id == 'la':
                                            home_team_id = 'lal'
                                        else:
                                            away_team_id = 'lal'
                                    else:  # If we're processing Clippers
                                        if home_team_id == 'la':
                                            home_team_id = 'lac'
                                        else:
                                            away_team_id = 'lac'
                    else:
                        # Failed to resolve teams, use opponent string parsing as fallback
                        print(f"Resolution failed, using string parsing as fallback")
                        raw_opponent = game['opponent'].strip()
                        is_away = '@' in raw_opponent.lower()
                        
                        # Parse opponent info
                        opponent_id, _ = self._parse_opponent_info(raw_opponent)
                        
                        if not opponent_id:
                            # Special case for LA
                            if 'la' in raw_opponent.lower() and not any(x in raw_opponent.lower() for x in ['lal', 'lac']):
                                opponent_id = self._resolve_la_team_by_event_id(event_id)
                                if not opponent_id:
                                    # Last resort: if we're processing Lakers, assume it's Clippers
                                    opponent_id = 'lac' if team_abbr == 'lal' else 'lal'
                            else:
                                print(f"Could not normalize opponent: {raw_opponent} for game {event_id}")
                                continue
                        
                        # Determine home and away teams based on @ symbol
                        home_team_id = opponent_id if is_away else team_abbr
                        away_team_id = team_abbr if is_away else opponent_id
                    
                    # Final validation - ensure we have valid team IDs
                    if not home_team_id or not away_team_id:
                        print(f"Invalid team IDs for event {event_id}: home={home_team_id}, away={away_team_id}")
                        continue
                    
                    # Generate data hash and save to database
                    data_hash = hashlib.sha256(json.dumps({
                        'event_id': event_id,
                        'date': game_date,
                        'home_team': home_team_id,
                        'away_team': away_team_id,
                    }, sort_keys=True).encode()).hexdigest()
                    
                    cursor.execute("""
                        INSERT INTO games 
                            (event_id, game_date, home_team_id, away_team_id, data_hash)
                        VALUES 
                            (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) 
                        DO UPDATE SET 
                            game_date = EXCLUDED.game_date,
                            home_team_id = EXCLUDED.home_team_id,
                            away_team_id = EXCLUDED.away_team_id,
                            data_hash = EXCLUDED.data_hash
                        RETURNING game_id;
                    """, (
                        event_id, 
                        game_date, 
                        home_team_id, 
                        away_team_id, 
                        data_hash
                    ))
                    
                    game_id = cursor.fetchone()[0]
                    saved_games += 1
                    print(f"Saved game_id {game_id} for event {event_id}: {home_team_id} vs {away_team_id} on {game_date}")
                    
                except Exception as e:
                    print(f"Error saving individual game {game.get('event_id', 'unknown')}: {str(e)}")
                    continue
            
            # Commit all successful game saves at once
            conn.commit()
            print(f"✓ Schedule saved successfully for {team_abbr} - {saved_games} games persisted")
            return saved_games
            
        except Exception as e:
            print(f"Critical error while saving schedule for {team_abbr}: {str(e)}")
            if cursor:
                conn.rollback()
            raise
  
    def _save_schedule_with_conn(self, team_abbr, games, conn):
        """Internal method to save schedule with existing connection"""
        try:
            with conn.cursor() as cursor:
                for game in games:
                    # Your existing save logic here
                    game_date = datetime.strptime(game['date'], '%a, %b %d')
                    cursor.execute(
                        "INSERT INTO games (event_id, game_date, ...) VALUES (%s, %s, ...)",
                        (game['event_id'], game_date, ...)
                    )
            
            print(f"✓ Saved schedule for {team_abbr}")
            
        except Exception as e:
            print(f"Error saving schedule: {e}")
            raise

    def cleanup(self):
        """Clean up resources by closing the WebDriver."""
        if hasattr(self, 'driver'):
            self.driver.quit()
        logging.info('WebDriver closed')
