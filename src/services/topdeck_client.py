"""TopDeck.gg API client with rate limiting and retry logic"""

import os
import time
import requests
from typing import Dict, List, Optional
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class TopDeckClient:
    """Client for interacting with TopDeck.gg API"""
    
    BASE_URL = "https://topdeck.gg/api"
    RATE_LIMIT_DELAY = 0.3  # 300ms between requests (200 req/min limit = 300ms per request)
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TopDeck API client
        
        Args:
            api_key: API key for authentication. If None, loads from TOPDECK_API_KEY env var.
        """
        self.api_key = api_key or os.getenv('TOPDECK_API_KEY')
        if not self.api_key:
            raise ValueError("TopDeck API key is required. Set TOPDECK_API_KEY environment variable.")
        
        self.last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': self.api_key,
            'Content-Type': 'application/json'
        })
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.RATE_LIMIT_DELAY:
            sleep_time = self.RATE_LIMIT_DELAY - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """
        Make a request with retry logic
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., '/v2/tournaments')
            **kwargs: Additional arguments to pass to requests
        
        Returns:
            JSON response as dictionary, or None on error
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        for attempt in range(self.MAX_RETRIES):
            try:
                self._rate_limit()
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                # Try to get error details from response body
                error_details = None
                try:
                    if hasattr(response, 'text') and response.text:
                        error_details = response.text
                    if hasattr(response, 'json'):
                        try:
                            error_json = response.json()
                            error_details = error_json
                        except (ValueError, AttributeError):
                            pass
                except Exception:
                    pass
                
                if response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                elif response.status_code == 401:
                    logger.error("Invalid API key")
                    raise ValueError("Invalid TopDeck API key")
                elif response.status_code == 400:
                    # Bad Request - log details for debugging
                    error_msg = f"Bad Request (400) for {url}"
                    if error_details:
                        error_msg += f": {error_details}"
                    if 'json' in kwargs:
                        error_msg += f" | Request body: {kwargs['json']}"
                    logger.error(error_msg)
                    raise ValueError(f"TopDeck API Bad Request: {error_details or 'Invalid request parameters'}")
                elif response.status_code == 429:
                    # Rate limited - wait longer
                    wait_time = self.RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                elif response.status_code >= 500:
                    # Server error - retry
                    wait_time = self.RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"HTTP error {response.status_code}: {e}"
                    if error_details:
                        error_msg += f" | Details: {error_details}"
                    logger.error(error_msg)
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed after {self.MAX_RETRIES} attempts: {e}")
                    raise
        
        return None
    
    def get_tournaments(
        self,
        game: str = "Magic: The Gathering",
        format: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
        last: Optional[int] = None,
        participant_min: Optional[int] = None,
        participant_max: Optional[int] = None,
        columns: Optional[List[str]] = None,
        rounds: Optional[bool] = None,
        tids: Optional[List[str]] = None
    ) -> Optional[List[Dict]]:
        """
        Get tournament data from TopDeck API
        
        Args:
            game: Game name (default: "Magic: The Gathering")
            format: Format name (e.g., "Standard", "Modern")
            start: Unix timestamp for earliest start date
            end: Unix timestamp for latest end date
            last: Number of days back from today
            participant_min: Minimum number of participants
            participant_max: Maximum number of participants
            columns: List of columns to include in response
            rounds: Whether to include round data
            tids: List of tournament IDs to fetch
        
        Returns:
            List of tournament dictionaries, or None on error
        """
        body = {
            'game': game
        }
        
        if format:
            body['format'] = format
        if start:
            body['start'] = start
        if end:
            body['end'] = end
        if last:
            body['last'] = last
        if participant_min:
            body['participantMin'] = participant_min
        if participant_max:
            body['participantMax'] = participant_max
        if columns:
            body['columns'] = columns
        if rounds is not None:
            body['rounds'] = rounds
        if tids:
            body['TID'] = tids if isinstance(tids, list) else [tids]
        
        try:
            data = self._request('POST', '/v2/tournaments', json=body)
            if data and isinstance(data, list):
                return data
            elif data and isinstance(data, dict) and 'data' in data:
                return data['data']
            return data
        except Exception as e:
            logger.error(f"Error fetching tournaments: {e}")
            return None
    
    def get_tournament_details(self, tournament_id: str) -> Optional[Dict]:
        """
        Get detailed information about a specific tournament
        
        Args:
            tournament_id: Tournament ID (TID)
        
        Returns:
            Tournament details dictionary, or None on error
        """
        try:
            return self._request('GET', f'/v2/tournaments/{tournament_id}')
        except Exception as e:
            logger.error(f"Error fetching tournament {tournament_id}: {e}")
            return None
    
    def get_tournament_rounds(self, tournament_id: str) -> Optional[List[Dict]]:
        """
        Get all rounds for a specific tournament
        
        Args:
            tournament_id: Tournament ID (TID)
        
        Returns:
            List of round dictionaries, or None on error
        """
        try:
            data = self._request('GET', f'/v2/tournaments/{tournament_id}/rounds')
            if data and isinstance(data, list):
                return data
            return data
        except Exception as e:
            logger.error(f"Error fetching rounds for tournament {tournament_id}: {e}")
            return None
    
    def get_tournament_latest_round(self, tournament_id: str) -> Optional[List[Dict]]:
        """
        Get the latest/current round for a specific tournament
        
        Args:
            tournament_id: Tournament ID (TID)
        
        Returns:
            List of table dictionaries for the latest round, or None on error
        """
        try:
            data = self._request('GET', f'/v2/tournaments/{tournament_id}/rounds/latest')
            if data and isinstance(data, list):
                return data
            return data
        except Exception as e:
            logger.error(f"Error fetching latest round for tournament {tournament_id}: {e}")
            return None

