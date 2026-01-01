"""
API Extractor

Extracts data from REST APIs with pagination, rate limiting, and retry handling.
Supports OAuth2, API Key, and Basic authentication.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
import logging
import time

import polars as pl

from .base_extractor import BaseExtractor, ExtractorConfig, ExtractionError


class AuthType(Enum):
    """Supported authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"


class PaginationType(Enum):
    """Supported pagination strategies."""
    NONE = "none"
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"
    LINK = "link"  # Following Link headers


@dataclass
class APIConfig(ExtractorConfig):
    """Configuration for API extraction."""
    # Endpoint settings
    base_url: str = ""
    endpoint: str = ""
    method: str = "GET"
    
    # Authentication
    auth_type: AuthType = AuthType.NONE
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"
    username: Optional[str] = None
    password: Optional[str] = None
    bearer_token: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    
    # Request settings
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    
    # Pagination
    pagination_type: PaginationType = PaginationType.NONE
    page_size: int = 100
    max_pages: Optional[int] = None  # Safety limit
    offset_param: str = "offset"
    page_param: str = "page"
    cursor_param: str = "cursor"
    limit_param: str = "limit"
    
    # Response parsing
    data_path: str = "data"  # JSON path to data array
    total_path: Optional[str] = "total"  # JSON path to total count
    cursor_response_path: Optional[str] = "next_cursor"
    
    # Rate limiting
    requests_per_second: float = 10.0
    rate_limit_header: str = "X-RateLimit-Remaining"
    retry_after_header: str = "Retry-After"
    
    # Custom response handler
    response_handler: Optional[Callable] = None


class APIExtractor(BaseExtractor):
    """
    Extractor for REST APIs.
    
    Supports:
    - Multiple authentication methods (API Key, Basic, OAuth2, Bearer)
    - Various pagination strategies (offset, page, cursor, Link headers)
    - Rate limiting with automatic backoff
    - Nested JSON response parsing
    
    Example:
        config = APIConfig(
            source_name="crm",
            target_table="customers",
            bronze_path=Path("/lakehouse/bronze"),
            base_url="https://api.crm.company.com/v2",
            endpoint="/customers",
            auth_type=AuthType.BEARER,
            bearer_token=os.getenv("CRM_TOKEN"),
            pagination_type=PaginationType.OFFSET,
            page_size=500,
            data_path="results.customers",
        )
        
        extractor = APIExtractor(config)
        df = extractor.extract()
        extractor.write_to_bronze(df)
    """
    
    def __init__(self, config: APIConfig):
        super().__init__(config)
        self.api_config = config
        self._session = None
        self._last_request_time = 0.0
        self._oauth_token = None
        self._oauth_expires_at = 0.0
    
    def _connect(self) -> None:
        """Initialize HTTP session with authentication."""
        import httpx
        
        self._session = httpx.Client(
            timeout=httpx.Timeout(self.config.timeout_seconds),
            follow_redirects=True,
        )
        
        # Set up authentication
        self._setup_authentication()
        
        # Set base headers
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self.api_config.headers,
        })
        
        self.logger.info(f"API session initialized for {self.api_config.base_url}")
    
    def _setup_authentication(self) -> None:
        """Configure authentication based on auth_type."""
        if self.api_config.auth_type == AuthType.API_KEY:
            self._session.headers[self.api_config.api_key_header] = self.api_config.api_key
            
        elif self.api_config.auth_type == AuthType.BASIC:
            import base64
            credentials = f"{self.api_config.username}:{self.api_config.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            self._session.headers["Authorization"] = f"Basic {encoded}"
            
        elif self.api_config.auth_type == AuthType.BEARER:
            self._session.headers["Authorization"] = f"Bearer {self.api_config.bearer_token}"
            
        elif self.api_config.auth_type == AuthType.OAUTH2:
            self._refresh_oauth_token()
    
    def _refresh_oauth_token(self) -> None:
        """Obtain or refresh OAuth2 token."""
        if time.time() < self._oauth_expires_at - 60:  # 60 second buffer
            return
        
        import httpx
        
        response = httpx.post(
            self.api_config.oauth2_token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.api_config.oauth2_client_id,
                "client_secret": self.api_config.oauth2_client_secret,
            },
        )
        response.raise_for_status()
        
        token_data = response.json()
        self._oauth_token = token_data["access_token"]
        self._oauth_expires_at = time.time() + token_data.get("expires_in", 3600)
        
        self._session.headers["Authorization"] = f"Bearer {self._oauth_token}"
        self.logger.info("OAuth2 token refreshed")
    
    def _extract(self) -> pl.DataFrame:
        """Fetch all pages and combine into single DataFrame."""
        all_data = []
        page_count = 0
        
        if self.api_config.pagination_type == PaginationType.NONE:
            # Single request
            data = self._make_request()
            all_data = self._extract_data_from_response(data)
        
        elif self.api_config.pagination_type == PaginationType.OFFSET:
            all_data = self._paginate_offset()
        
        elif self.api_config.pagination_type == PaginationType.PAGE:
            all_data = self._paginate_page()
        
        elif self.api_config.pagination_type == PaginationType.CURSOR:
            all_data = self._paginate_cursor()
        
        elif self.api_config.pagination_type == PaginationType.LINK:
            all_data = self._paginate_link()
        
        if not all_data:
            self.logger.warning("API returned no data")
            return pl.DataFrame()
        
        # Convert to Polars DataFrame
        return pl.DataFrame(all_data)
    
    def _paginate_offset(self) -> List[Dict]:
        """Handle offset-based pagination."""
        all_data = []
        offset = 0
        page_count = 0
        
        while True:
            params = {
                **self.api_config.params,
                self.api_config.offset_param: offset,
                self.api_config.limit_param: self.api_config.page_size,
            }
            
            response = self._make_request(params=params)
            data = self._extract_data_from_response(response)
            
            if not data:
                break
            
            all_data.extend(data)
            offset += len(data)
            page_count += 1
            
            self.logger.info(f"Page {page_count}: fetched {len(data)} records, total: {len(all_data)}")
            
            # Check limits
            if len(data) < self.api_config.page_size:
                break  # Last page
            
            if self.api_config.max_pages and page_count >= self.api_config.max_pages:
                self.logger.warning(f"Max pages limit ({self.api_config.max_pages}) reached")
                break
        
        return all_data
    
    def _paginate_page(self) -> List[Dict]:
        """Handle page-number based pagination."""
        all_data = []
        page = 1
        
        while True:
            params = {
                **self.api_config.params,
                self.api_config.page_param: page,
                self.api_config.limit_param: self.api_config.page_size,
            }
            
            response = self._make_request(params=params)
            data = self._extract_data_from_response(response)
            
            if not data:
                break
            
            all_data.extend(data)
            
            self.logger.info(f"Page {page}: fetched {len(data)} records, total: {len(all_data)}")
            
            if len(data) < self.api_config.page_size:
                break
            
            if self.api_config.max_pages and page >= self.api_config.max_pages:
                break
            
            page += 1
        
        return all_data
    
    def _paginate_cursor(self) -> List[Dict]:
        """Handle cursor-based pagination."""
        all_data = []
        cursor = None
        page_count = 0
        
        while True:
            params = {
                **self.api_config.params,
                self.api_config.limit_param: self.api_config.page_size,
            }
            
            if cursor:
                params[self.api_config.cursor_param] = cursor
            
            response = self._make_request(params=params)
            data = self._extract_data_from_response(response)
            
            if not data:
                break
            
            all_data.extend(data)
            page_count += 1
            
            # Get next cursor
            cursor = self._get_nested_value(
                response, 
                self.api_config.cursor_response_path
            )
            
            self.logger.info(f"Page {page_count}: fetched {len(data)} records")
            
            if not cursor:
                break
            
            if self.api_config.max_pages and page_count >= self.api_config.max_pages:
                break
        
        return all_data
    
    def _paginate_link(self) -> List[Dict]:
        """Handle Link header based pagination (RFC 5988)."""
        all_data = []
        url = f"{self.api_config.base_url}{self.api_config.endpoint}"
        page_count = 0
        
        while url:
            response = self._make_request(full_url=url)
            data = self._extract_data_from_response(response["body"])
            
            if data:
                all_data.extend(data)
            
            page_count += 1
            self.logger.info(f"Page {page_count}: fetched {len(data)} records")
            
            # Parse Link header for next URL
            url = self._parse_link_header(response.get("headers", {}))
            
            if self.api_config.max_pages and page_count >= self.api_config.max_pages:
                break
        
        return all_data
    
    def _make_request(
        self, 
        params: Optional[Dict] = None, 
        full_url: Optional[str] = None
    ) -> Any:
        """Execute HTTP request with rate limiting and retries."""
        # Rate limiting
        self._apply_rate_limit()
        
        # Refresh OAuth token if needed
        if self.api_config.auth_type == AuthType.OAUTH2:
            self._refresh_oauth_token()
        
        url = full_url or f"{self.api_config.base_url}{self.api_config.endpoint}"
        
        try:
            if self.api_config.method.upper() == "GET":
                response = self._session.get(url, params=params or self.api_config.params)
            elif self.api_config.method.upper() == "POST":
                response = self._session.post(
                    url, 
                    params=params,
                    json=self.api_config.body
                )
            else:
                raise ExtractionError(f"Unsupported HTTP method: {self.api_config.method}")
            
            # Handle rate limit responses
            if response.status_code == 429:
                retry_after = int(response.headers.get(self.api_config.retry_after_header, 60))
                self.logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                return self._make_request(params, full_url)
            
            response.raise_for_status()
            
            if full_url and self.api_config.pagination_type == PaginationType.LINK:
                return {
                    "body": response.json(),
                    "headers": dict(response.headers),
                }
            
            return response.json()
            
        except Exception as e:
            raise ExtractionError(f"API request failed: {str(e)}")
    
    def _apply_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        min_interval = 1.0 / self.api_config.requests_per_second
        elapsed = time.time() - self._last_request_time
        
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        
        self._last_request_time = time.time()
    
    def _extract_data_from_response(self, response: Dict) -> List[Dict]:
        """Extract data array from nested JSON response."""
        if self.api_config.response_handler:
            return self.api_config.response_handler(response)
        
        data = self._get_nested_value(response, self.api_config.data_path)
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return []
    
    def _get_nested_value(self, data: Dict, path: Optional[str]) -> Any:
        """Get value from nested dictionary using dot notation path."""
        if not path:
            return data
        
        keys = path.split(".")
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        
        return value
    
    def _parse_link_header(self, headers: Dict) -> Optional[str]:
        """Parse Link header to find 'next' URL."""
        link_header = headers.get("Link", headers.get("link", ""))
        
        if not link_header:
            return None
        
        # Parse RFC 5988 Link header
        for link in link_header.split(","):
            parts = link.strip().split(";")
            if len(parts) >= 2:
                url = parts[0].strip().strip("<>")
                rel = parts[1].strip()
                if 'rel="next"' in rel or "rel=next" in rel:
                    return url
        
        return None
    
    def _disconnect(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self.logger.info("API session closed")


# Convenience factory functions
def create_rest_api_extractor(
    source_name: str,
    target_table: str,
    bronze_path: Path,
    base_url: str,
    endpoint: str,
    bearer_token: str,
    **kwargs,
) -> APIExtractor:
    """Factory function for simple REST API extraction."""
    config = APIConfig(
        source_name=source_name,
        target_table=target_table,
        bronze_path=bronze_path,
        base_url=base_url,
        endpoint=endpoint,
        auth_type=AuthType.BEARER,
        bearer_token=bearer_token,
        **kwargs,
    )
    return APIExtractor(config)
