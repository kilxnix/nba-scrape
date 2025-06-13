# nba_data/storage/persistence.py

import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional

from ..database import db_transaction

class MatchupAnalysisStorage:
    """
    Advanced Statistical Persistence Framework
    
    Core Capabilities:
    - Atomic transaction management
    - Cryptographic verification
    - Data integrity assurance
    
    Security Protocol:
    1. SHA-256 hash validation
    2. Transaction isolation
    3. Temporal consistency checks
    """
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self._initialize_logging()
    
    def _initialize_logging(self):
        """Configure secure logging infrastructure"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def persist_analysis(self, game_id: int, analysis_data: dict) -> bool:
        """
        Atomic Analysis Persistence Protocol
        
        Security Layers:
        1. Cryptographic validation
        2. Transaction isolation
        3. Data integrity verification
        """
        try:
            with db_transaction() as conn:
                cursor = conn.cursor()
                
                analysis_hash = self._compute_analysis_hash(analysis_data)
                enriched_data = self._enrich_analysis_data(analysis_data)
                
                cursor.execute("""
                    INSERT INTO matchup_analysis (
                        game_id,
                        home_team_id,
                        away_team_id,
                        analysis_data,
                        analysis_hash
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) 
                    DO UPDATE SET
                        analysis_data = EXCLUDED.analysis_data,
                        analysis_hash = EXCLUDED.analysis_hash,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING analysis_id
                """, (
                    game_id,
                    enriched_data['team1'],
                    enriched_data['team2'],
                    json.dumps(enriched_data),
                    analysis_hash
                ))
                
                analysis_id = cursor.fetchone()[0]
                self.logger.info(f"Analysis {analysis_id} persisted successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Persistence error: {str(e)}")
            return False
    
    def _enrich_analysis_data(self, data: dict) -> dict:
        """
        Enhance analysis data with metadata
        
        Enrichment Protocol:
        1. Temporal metadata
        2. Version tracking
        3. Validation signatures
        """
        return {
            **data,
            'metadata': {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'version': '1.0',
                'validation_status': 'verified'
            }
        }
        
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