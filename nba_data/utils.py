from datetime import datetime


def current_season_start_year(today: datetime = None) -> int:
    """Return the NBA season start year based on today's date."""
    if today is None:
        today = datetime.utcnow()
    return today.year if today.month >= 7 else today.year - 1

def parse_game_date(date_str: str, season_start_year: int | None = None):
    """Parse an ESPN schedule date and assign the correct season year.

    ``season_start_year`` indicates the NBA season start (e.g. ``2024`` for the
    2024-25 season). If omitted, it is calculated from today's date.
    
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
        if season_start_year is None:
            season_start_year = current_season_start_year()

        # Months Oct-Dec belong to the season start year, Jan-Jun to the next
        if temp_date.month >= 10:
            year = season_start_year
        else:
            year = season_start_year + 1

        parsed_date = temp_date.replace(year=year)
        print(f"Parsed date '{date_str}' to: {parsed_date.strftime('%Y-%m-%d')}")
        return parsed_date
            
    except ValueError as e:
        print(f"Error parsing date string '{date_str}': {e}")
        return None
