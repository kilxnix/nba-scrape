from datetime import datetime

def parse_game_date(date_str):
    """
    Parse ESPN schedule date format for NBA season games.

    Handles season transition from 2024 to 2025:
    - Games in Oct-Dec are assigned to 2024
    - Games in Jan-June are assigned to 2025

    Args:
        date_str (str): Date string in ESPN format (e.g., 'Tue, Oct 24')

    Returns:
        datetime: Parsed date with correct year assignment
    """
    try:
        # Split weekday from month/day and strip whitespace
        weekday, date_part = [part.strip() for part in date_str.split(',')]
        
        # Parse month and day
        temp_date = datetime.strptime(date_part, '%b %d')
        
        # Assign year based on month:
        # Oct-Dec = 2024
        # Jan-June = 2025
        year = 2024 if temp_date.month >= 10 else 2025
        
        parsed_date = temp_date.replace(year=year)
        print(f"Parsed date '{date_str}' to: {parsed_date.strftime('%Y-%m-%d')}")
        return parsed_date
            
    except ValueError as e:
        print(f"Error parsing date string '{date_str}': {e}")
        return None