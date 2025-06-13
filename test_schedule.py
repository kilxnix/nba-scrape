# test_schedule.py
import os
from dotenv import load_dotenv
from nba_data.fetcher import NBAScheduleFetcher
from bs4 import BeautifulSoup
import time

load_dotenv()  # Load environment variables

def get_db_connection_string():
    """Build connection string from env vars"""
    return (
        f"postgresql://{os.getenv('user')}:{os.getenv('password')}"
        f"@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}"
    )

def test_opponent_parsing():
    """Test opponent string parsing with real ESPN data"""
    conn_string = get_db_connection_string()
    fetcher = NBAScheduleFetcher(conn_string)
    
    # Fetch real schedule HTML
    url = "https://www.espn.com/nba/team/schedule/_/name/bos/boston-celtics"
    fetcher.driver.get(url)
    time.sleep(5)  # Let JavaScript load
    
    html = fetcher.driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all schedule rows
    rows = soup.find_all('tr', class_='Table__TR')
    
    print("\nESPN Schedule Format Analysis:")
    print("=" * 50)
    
    for row in rows[:10]:  # Analyze first 10 games
        try:
            # Get opponent cell
            cells = row.find_all('td', class_='Table__TD')
            if len(cells) < 2:
                continue
                
            date_cell = cells[0]
            opponent_cell = cells[1]
            
            # Extract raw text
            date_text = date_cell.text.strip()
            opponent_text = opponent_cell.text.strip()
            
            print(f"\nGame Entry:")
            print(f"Raw date: '{date_text}'")
            print(f"Raw opponent: '{opponent_text}'")
            
            # Test opponent parsing
            is_away = '@' in opponent_text
            team = opponent_text.replace('@', '').replace('vs', '').strip()
            
            print(f"Cleaned team: '{team}'")
            print(f"Is away: {is_away}")
            
            # Get event ID if present
            game_link = row.find('a', href=lambda x: x and 'gameId' in x)
            if game_link:
                event_id = game_link['href'].split('gameId/')[1].split('/')[0]
                print(f"Event ID: {event_id}")
            
        except Exception as e:
            print(f"Row parse error: {e}")
            continue
            
    fetcher.driver.quit()

if __name__ == '__main__':
    test_opponent_parsing()