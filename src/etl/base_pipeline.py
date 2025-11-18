"""Base protocol for ETL pipelines"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BasePipeline(ABC):
    """Base class for ETL pipelines with standardized interface"""
    
    @abstractmethod
    def load_initial(self, **kwargs) -> Dict[str, Any]:
        """
        Perform initial/full load of data.
        
        Returns:
            Dictionary with keys:
            - success: bool - Whether load completed successfully
            - objects_loaded: int - Number of objects loaded
            - objects_processed: int - Total objects processed
            - errors: int - Number of errors
        """
        pass
    
    @abstractmethod
    def load_incremental(self, **kwargs) -> Dict[str, Any]:
        """
        Perform incremental load of data since last load.
        
        Returns:
            Dictionary with keys:
            - success: bool - Whether load completed successfully
            - objects_loaded: int - Number of objects loaded
            - objects_processed: int - Total objects processed
            - errors: int - Number of errors
        """
        pass

