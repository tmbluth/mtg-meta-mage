"""Scryfall API client with rate limiting and retry logic"""

import time
import requests
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ScryfallClient:
    """Client for interacting with Scryfall API"""
    
    BASE_URL = "https://api.scryfall.com"
    RATE_LIMIT_DELAY = 0.1  # 100ms between requests (50-100 req/sec limit)
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    
    def __init__(self):
        self.last_request_time = 0.0
        self.session = requests.Session()
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.RATE_LIMIT_DELAY:
            sleep_time = self.RATE_LIMIT_DELAY - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make a request with retry logic"""
        url = f"{self.BASE_URL}{endpoint}"
        
        for attempt in range(self.MAX_RETRIES):
            try:
                self._rate_limit()
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
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
                    logger.error(f"HTTP error {response.status_code}: {e}")
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
    
    def get_bulk_data_url(self, data_type: str = "oracle_cards") -> Optional[str]:
        """
        Get the URL for downloading bulk data from Scryfall.
        
        Args:
            data_type: Type of bulk data to download ("oracle_cards" or "rulings")
        
        Returns:
            Download URI for the specified bulk data type, or None if not found
        """
        try:
            data = self._request("GET", "/bulk-data")
            if not data:
                return None
            
            # Find the specified bulk data file
            for item in data.get("data", []):
                if item.get("type") == data_type:
                    return item.get("download_uri")
            
            logger.error(f"{data_type} bulk data not found")
            return None
        except Exception as e:
            logger.error(f"Error fetching bulk data URL: {e}")
            return None
    
    def download_bulk_data(self, data_type: str = "oracle_cards", url: Optional[str] = None) -> Optional[Dict]:
        """
        Download and parse Scryfall bulk data. Save locally to data/{data_type}/ folder.
        
        Returns structured data ready for database insertion. The 'data' field contains
        a list of card/ruling dictionaries that can be transformed using transform_card_to_db_row()
        or processed directly for database operations.
        
        Args:
            data_type: Type of bulk data to download ("oracle_cards" or "rulings")
            url: Download URL. If None, will fetch the current URL from the API.
        
        Returns:
            Dictionary with 'data' (list of items) and 'file_path' keys, or None on error.
            The 'data' field contains structured JSON objects ready for transformation.
        """
        import os
        import json
        from urllib.parse import urlparse

        if url is None:
            url = self.get_bulk_data_url(data_type)
        
        if not url:
            return None
        
        try:
            logger.info(f"Downloading {data_type} from {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Parse JSON data
            data = response.json()
            
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and "data" in data:
                items = data["data"]
            else:
                logger.error(f"Unexpected data format from {data_type} download")
                return None
            
            logger.info(f"Downloaded {len(items)} {data_type}")
            
            # Extract filename from URL (e.g., oracle-cards-20251105100313.json)
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            
            # Save data locally under data/{data_type}/ folder
            data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', data_type)
            data_folder = os.path.abspath(data_folder)
            os.makedirs(data_folder, exist_ok=True)
            
            # Save with the original timestamped filename
            file_path = os.path.join(data_folder, filename)
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(items, f)
                logger.info(f"{data_type} saved to {file_path}")
            except Exception as save_e:
                logger.error(f"Failed to save {data_type} to {file_path}: {save_e}")
                return None

            return {"data": items, "file_path": file_path}
        except Exception as e:
            logger.error(f"Error downloading {data_type}: {e}")
            return None
    
    def download_oracle_cards(self, url: Optional[str] = None) -> Optional[Dict]:
        """
        Download Oracle Cards bulk data.
        Convenience wrapper for download_bulk_data("oracle_cards").
        
        Args:
            url: Download URL. If None, will fetch the current URL from the API.
        
        Returns:
            Dictionary with 'data' (list of cards) and 'file_path' keys, or None on error
        """
        return self.download_bulk_data("oracle_cards", url)
    
    def download_rulings(self, url: Optional[str] = None) -> Optional[Dict]:
        """
        Download Rulings bulk data.
        Convenience wrapper for download_bulk_data("rulings").
        
        Args:
            url: Download URL. If None, will fetch the current URL from the API.
        
        Returns:
            Dictionary with 'data' (list of rulings) and 'file_path' keys, or None on error
        """
        return self.download_bulk_data("rulings", url)
    
    def join_cards_with_rulings(self, cards: List[Dict], rulings: List[Dict]) -> List[Dict]:
        """
        Join oracle cards with their rulings by oracle_id.
        
        Args:
            cards: List of oracle card dictionaries
            rulings: List of ruling dictionaries
        
        Returns:
            List of cards with rulings added as a list field
        """
        logger.info(f"Joining {len(cards)} cards with {len(rulings)} rulings...")
        
        # Build a mapping of oracle_id to list of ruling comments
        rulings_by_oracle_id = {}
        for ruling in rulings:
            oracle_id = ruling.get("oracle_id")
            if oracle_id:
                if oracle_id not in rulings_by_oracle_id:
                    rulings_by_oracle_id[oracle_id] = []
                rulings_by_oracle_id[oracle_id].append(ruling.get("comment", ""))
        
        # Add rulings to each card
        cards_with_rulings = []
        for card in cards:
            card_copy = card.copy()
            oracle_id = card.get("oracle_id")
            if oracle_id and oracle_id in rulings_by_oracle_id:
                card_copy["rulings"] = rulings_by_oracle_id[oracle_id]
            else:
                card_copy["rulings"] = []
            cards_with_rulings.append(card_copy)
        
        logger.info(f"Joined cards with rulings successfully")
        return cards_with_rulings
    
    def concatenate_rulings(self, rulings: List[str]) -> str:
        """
        Concatenate a list of ruling comments into a comma-separated string.
        
        Args:
            rulings: List of ruling comment strings
        
        Returns:
            Comma-separated string of rulings, or empty string if no rulings
        """
        if not rulings:
            return ""
        # Filter out empty strings and join with commas
        valid_rulings = [r.strip() for r in rulings if r and r.strip()]
        return ", ".join(valid_rulings)
    
    def transform_card_to_db_row(self, card: Dict) -> Dict:
        """
        Transform a Scryfall oracle card object to database row format.
        
        Maps Scryfall card fields to database columns:
        - id -> card_id
        - set -> set
        - collector_number -> collector_num
        - name -> name
        - oracle_text -> oracle_text
        - rulings (list) -> rulings (comma-separated string)
        - type_line -> type_line
        - mana_cost -> mana_cost
        - cmc -> cmc
        - color_identity -> color_identity (array)
        - scryfall_uri -> scryfall_uri
        
        Args:
            card: Scryfall oracle card dictionary
        
        Returns:
            Dictionary with database column names as keys
        """
        # Handle rulings - if it's a list, concatenate; if already string, use as-is
        rulings_value = card.get("rulings", [])
        if isinstance(rulings_value, list):
            rulings_text = self.concatenate_rulings(rulings_value)
        else:
            rulings_text = str(rulings_value) if rulings_value else ""
        
        # Handle color_identity - ensure it's a list
        color_identity = card.get("color_identity", [])
        if not isinstance(color_identity, list):
            color_identity = []
        
        return {
            "card_id": card.get("id", ""),
            "set": card.get("set"),
            "collector_num": card.get("collector_number"),
            "name": card.get("name", ""),
            "oracle_text": card.get("oracle_text"),
            "rulings": rulings_text,
            "type_line": card.get("type_line"),
            "mana_cost": card.get("mana_cost"),
            "cmc": card.get("cmc"),
            "color_identity": color_identity,
            "scryfall_uri": card.get("scryfall_uri")
        }
    
    def get_card_price(self, card_name: str) -> Optional[float]:
        """Get current price for a card by name"""
        try:
            # Use the named card endpoint
            data = self._request("GET", f"/cards/named", params={"exact": card_name})
            if not data:
                return None
            
            # Try to get USD price from prices object
            prices = data.get("prices", {})
            usd_price = prices.get("usd")
            
            if usd_price:
                return float(usd_price)
            
            # Try foil price if regular price not available
            usd_foil = prices.get("usd_foil")
            if usd_foil:
                return float(usd_foil)
            
            logger.warning(f"No price found for {card_name}")
            return None
        except Exception as e:
            logger.error(f"Error fetching price for {card_name}: {e}")
            return None
    
    def get_card_by_name(self, card_name: str) -> Optional[Dict]:
        """Get full card data by name"""
        try:
            data = self._request("GET", f"/cards/named", params={"exact": card_name})
            return data
        except Exception as e:
            logger.error(f"Error fetching card {card_name}: {e}")
            return None

