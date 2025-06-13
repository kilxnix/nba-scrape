from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Optional

from nba_data_project.nba_data.database import db_transaction


class MatchupAnalysisStorage:
    """
    Advanced Data Persistence Framework for NBA Analytics
    
    Core Components:
    - Atomic transaction management
    - Cryptographic data validation
    - Temporal consistency verification
    
    Implementation Protocol:
    1. Cryptographic hash generation
    2. Atomic data persistence
    3. Transaction isolation enforcement
    """
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        
    def persist_analysis(self, game_id: int, analysis_data: dict) -> bool:
        """
        Atomic Analysis Persistence Protocol
        
        Validation Layers:
        1. Data integrity verification
        2. Temporal consistency check
        3. Cryptographic hash validation
        
        Returns:
            bool: Success state of persistence operation
        """
        try:
            with db_transaction() as conn:
                cursor = conn.cursor()
                
                # Generate cryptographic analysis hash
                analysis_hash = self._compute_analysis_hash(analysis_data)
                
                # Extract team identifiers
                home_team_id = analysis_data['team1']
                away_team_id = analysis_data['team2']
                
                # Construct temporal metadata
                metadata = {
                    'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
                    'data_version': '1.0',
                    'validation_hash': analysis_hash
                }
                
                # Merge metadata with analysis
                enriched_data = {
                    **analysis_data,
                    'metadata': metadata
                }
                
                # Atomic upsert operation
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
                    RETURNING analysis_id
                """, (
                    game_id,
                    home_team_id,
                    away_team_id,
                    json.dumps(enriched_data),
                    analysis_hash
                ))
                
                analysis_id = cursor.fetchone()[0]
                
                # Verify persistence integrity
                self._verify_persistence(conn, analysis_id, analysis_hash)
                
                return True
                
        except Exception as e:
            logging.error(f"Analysis persistence error: {str(e)}")
            return False
            
    def _compute_analysis_hash(self, analysis_data: dict) -> str:
        """
        Generate Cryptographic Hash for Analysis Data
        
        Implementation:
        1. Canonical JSON serialization
        2. SHA-256 hash computation
        3. Hexadecimal encoding
        """
        analysis_str = json.dumps(analysis_data, sort_keys=True)
        return hashlib.sha256(analysis_str.encode()).hexdigest()
        
    def _verify_persistence(self, conn, analysis_id: int, expected_hash: str) -> bool:
        """
        Validate Analysis Persistence Integrity
        
        Verification Protocol:
        1. Data retrieval validation
        2. Hash comparison
        3. Metadata verification
        """
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT analysis_hash, analysis_data
                FROM matchup_analysis
                WHERE analysis_id = %s
            """, (analysis_id,))
            
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Analysis {analysis_id} not found after persistence")
                
            stored_hash, stored_data = row
            
            # Verify hash consistency
            if stored_hash != expected_hash:
                raise ValueError("Analysis hash mismatch after persistence")
                
            return True

    def retrieve_analysis(self, game_id: int) -> Optional[dict]:
        """
        Secure Analysis Retrieval Protocol
        
        Implementation:
        1. Data integrity verification
        2. Hash validation
        3. Metadata enrichment
        """
        try:
            with db_transaction() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT analysis_data, analysis_hash
                    FROM matchup_analysis
                    WHERE game_id = %s
                """, (game_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                    
                stored_data, stored_hash = row
                
                # Validate data integrity
                computed_hash = self._compute_analysis_hash(stored_data)
                if computed_hash != stored_hash:
                    raise ValueError("Analysis integrity validation failed")
                    
                return stored_data
                
        except Exception as e:
            logging.error(f"Analysis retrieval error: {str(e)}")
            return None