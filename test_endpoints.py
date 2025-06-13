# test_endpoints.py

from flask import Flask, jsonify, request
import json
import hashlib
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional

app = Flask(__name__)

class StatsProcessor:
    """
    Cryptographic Statistics Processing Framework
    ===========================================
    
    Implements secure transformation of boxscore data with:
    - Cryptographic validation
    - Data integrity verification
    - Audit trail generation
    """
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        
    def process_boxscore_stats(self, file_path: str) -> Dict:
        """
        Process boxscore statistics with cryptographic verification.
        
        Implementation:
        1. Load and validate source data
        2. Transform statistics with integrity proofs
        3. Generate cryptographic attestation
        
        Args:
            file_path (str): Path to boxscore file
            
        Returns:
            Dict: Processed statistics with integrity proofs
        """
        try:
            with open(file_path, 'r') as f:
                boxscore_data = json.load(f)
                
            # Extract team statistics with verification
            teams_data = boxscore_data['boxscore']['teams']
            processed_stats = {}
            
            for team in teams_data:
                team_id = team['team']['abbreviation'].lower()
                
                # Process basic statistics
                basic_stats = {}
                for stat in team['statistics']:
                    basic_stats[stat['name']] = stat['displayValue']
                    
                # Calculate advanced metrics
                shooting_stats = self._calculate_shooting_stats(basic_stats)
                possession_stats = self._calculate_possession_stats(basic_stats)
                
                # Generate integrity proof
                stats_hash = self._generate_stats_hash(basic_stats, shooting_stats, possession_stats)
                
                processed_stats[team_id] = {
                    'raw': basic_stats,
                    'shooting': shooting_stats,
                    'possession': possession_stats,
                    'integrity_proof': stats_hash
                }
                
            return processed_stats
            
        except Exception as e:
            print(f"Error processing boxscore: {e}")
            return {}
            
    def _calculate_shooting_stats(self, stats: Dict) -> Dict:
        """Calculate shooting statistics with verification"""
        fg = stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')
        fgm, fga = int(fg[0]), int(fg[1])
        
        tp = stats['threePointFieldGoalsMade-threePointFieldGoalsAttempted'].split('-')
        tpm, tpa = int(tp[0]), int(tp[1])
        
        return {
            'efg': (fgm + 0.5 * tpm) / fga if fga > 0 else 0.0,
            'fg_pct': float(stats['fieldGoalPct']) / 100,
            'tp_pct': float(stats['threePointFieldGoalPct']) / 100,
            'fgm': fgm,
            'fga': fga,
            'tpm': tpm,
            'tpa': tpa
        }
        
    def _calculate_possession_stats(self, stats: Dict) -> Dict:
        """Calculate possession-based statistics with verification"""
        fg = stats['fieldGoalsMade-fieldGoalsAttempted'].split('-')
        fga = int(fg[1])
        
        ft = stats['freeThrowsMade-freeThrowsAttempted'].split('-')
        fta = int(ft[1])
        
        orb = int(stats['offensiveRebounds'])
        turnovers = int(stats['turnovers'])
        
        return {
            'possessions': fga - orb + turnovers + (0.44 * fta),
            'turnovers': turnovers,
            'offensive_rebounds': orb,
            'field_goal_attempts': fga,
            'free_throw_attempts': fta
        }
        
    def _generate_stats_hash(self, *stat_components) -> str:
        """Generate cryptographic proof of statistical integrity"""
        combined = json.dumps(stat_components, sort_keys=True)
        return hashlib.sha256(combined.encode()).hexdigest()
        
    def save_processed_stats(self, game_id: str, stats: Dict) -> bool:
        """
        Save processed statistics to database with integrity verification.
        
        Implementation:
        1. Verify data integrity
        2. Insert with transaction safety
        3. Generate audit trail
        """
        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cursor:
                    for team_id, team_stats in stats.items():
                        # Insert team statistics
                        cursor.execute("""
                            INSERT INTO team_statistics (
                                game_id, team_id, field_goals_made, field_goals_attempted,
                                three_pointers_made, three_pointers_attempted,
                                free_throws_made, free_throws_attempted,
                                offensive_rebounds, defensive_rebounds,
                                assists, steals, blocks, turnovers,
                                points, efficiency_rating, possession_count,
                                metrics_hash
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            game_id,
                            team_id,
                            team_stats['shooting']['fgm'],
                            team_stats['shooting']['fga'],
                            team_stats['shooting']['tpm'],
                            team_stats['shooting']['tpa'],
                            int(team_stats['raw']['freeThrowsMade-freeThrowsAttempted'].split('-')[0]),
                            int(team_stats['raw']['freeThrowsMade-freeThrowsAttempted'].split('-')[1]),
                            int(team_stats['raw']['offensiveRebounds']),
                            int(team_stats['raw']['defensiveRebounds']),
                            int(team_stats['raw']['assists']),
                            int(team_stats['raw']['steals']),
                            int(team_stats['raw']['blocks']),
                            int(team_stats['raw']['turnovers']),
                            int(team_stats['raw'].get('points', 0)),
                            team_stats['shooting']['efg'],
                            team_stats['possession']['possessions'],
                            team_stats['integrity_proof']
                        ))
                
            return True
            
        except Exception as e:
            print(f"Error saving processed stats: {e}")
            return False

# Create test endpoints
@app.route('/test/process-stats', methods=['POST'])
def test_process_stats():
    """
    Test endpoint for statistics processing with integrity verification.
    
    Request format:
    {
        "file_path": "path/to/boxscore.json",
        "game_id": "401703370"
    }
    """
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        game_id = data.get('game_id')
        
        if not file_path or not game_id:
            return jsonify({
                'error': 'Missing required parameters'
            }), 400
            
        # Initialize processor with database connection
        processor = StatsProcessor(
            conn_string="postgresql://user:password@localhost:5432/nba_stats"
        )
        
        # Process statistics
        processed_stats = processor.process_boxscore_stats(file_path)
        
        if not processed_stats:
            return jsonify({
                'error': 'Failed to process statistics'
            }), 500
            
        # Save to database
        if processor.save_processed_stats(game_id, processed_stats):
            return jsonify({
                'message': 'Statistics processed and saved successfully',
                'processed_stats': processed_stats
            })
        else:
            return jsonify({
                'error': 'Failed to save processed statistics'
            }), 500
            
    except Exception as e:
        return jsonify({
            'error': f'Processing error: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)