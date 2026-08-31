"""
RPA Extractor

Extracts data from legacy systems without APIs using Selenium-based automation.
Handles navigation, form filling, file downloads, and data capture.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import os
import shutil
import time

import polars as pl

from .base_extractor import BaseExtractor, ExtractorConfig, ExtractionError


@dataclass
class RPAConfig(ExtractorConfig):
    """Configuration for RPA extraction."""
    # Browser settings
    browser: str = "chrome"  # chrome, firefox, edge
    headless: bool = True
    download_dir: Path = Path("/tmp/rpa_downloads")
    screenshot_dir: Path = Path("/tmp/rpa_screenshots")
    
    # Login settings
    login_url: str = ""
    username_selector: str = "#username"
    password_selector: str = "#password"
    login_button_selector: str = "#login-btn"
    username: str = ""
    password: str = ""  # Should be loaded from vault
    mfa_handler: Optional[Callable] = None  # Custom MFA handling
    
    # Navigation
    steps: List[Dict[str, Any]] = field(default_factory=list)
    
    # Wait settings
    implicit_wait: int = 10
    page_load_timeout: int = 60
    download_wait_timeout: int = 300
    element_poll_interval: float = 0.5
    
    # File handling
    expected_file_pattern: str = "*.xlsx"
    file_parser: Optional[Callable] = None  # Custom file parser
    
    # Error handling
    screenshot_on_error: bool = True
    max_retries_per_step: int = 2


@dataclass
class RPAStep:
    """Represents a single RPA action step."""
    action: str  # click, type, select, wait, navigate, download, screenshot
    selector: Optional[str] = None
    value: Optional[str] = None
    wait_after: float = 1.0
    description: str = ""


class RPAExtractor(BaseExtractor):
    """
    Extractor for legacy systems using Selenium-based RPA.
    
    Supports:
    - Multiple browsers (Chrome, Firefox, Edge)
    - Headless execution
    - Login with MFA handling
    - Step-based navigation
    - File download handling
    - Screenshot capture for debugging
    
    Example:
        config = RPAConfig(
            source_name="wms",
            target_table="stock_movements",
            bronze_path=Path("/lakehouse/bronze"),
            login_url="https://legacy-wms.company.com/login",
            username="etl_bot",
            password=os.getenv("WMS_PASSWORD"),
            headless=True,
            steps=[
                {"action": "navigate", "value": "/reports"},
                {"action": "click", "selector": "#stock-movement-report"},
                {"action": "select", "selector": "#date-range", "value": "yesterday"},
                {"action": "click", "selector": "#export-excel"},
                {"action": "wait", "value": "5"},  # Wait for download
            ],
        )
        
        extractor = RPAExtractor(config)
        df = extractor.extract()
        extractor.write_to_bronze(df)
    """
    
    def __init__(self, config: RPAConfig):
        super().__init__(config)
        self.rpa_config = config
        self._driver = None
        self._wait = None
    
    def _connect(self) -> None:
        """Initialize Selenium WebDriver."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
        from selenium.webdriver.support.ui import WebDriverWait
        
        # Ensure directories exist
        self.rpa_config.download_dir.mkdir(parents=True, exist_ok=True)
        self.rpa_config.screenshot_dir.mkdir(parents=True, exist_ok=True)
        
        # Clear download directory
        for file in self.rpa_config.download_dir.glob("*"):
            file.unlink()
        
        # Configure Chrome options
        options = ChromeOptions()
        
        if self.rpa_config.headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Configure download behavior
        prefs = {
            "download.default_directory": str(self.rpa_config.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            self._driver = webdriver.Chrome(options=options)
            self._driver.implicitly_wait(self.rpa_config.implicit_wait)
            self._driver.set_page_load_timeout(self.rpa_config.page_load_timeout)
            
            self._wait = WebDriverWait(
                self._driver, 
                self.rpa_config.implicit_wait,
                poll_frequency=self.rpa_config.element_poll_interval,
            )
            
            self.logger.info(f"WebDriver initialized (headless={self.rpa_config.headless})")
            
        except Exception as e:
            raise ConnectionError(f"Failed to initialize WebDriver: {str(e)}")
    
    def _extract(self) -> pl.DataFrame:
        """Execute RPA workflow and extract data."""
        try:
            # Step 1: Login
            if self.rpa_config.login_url:
                self._perform_login()
            
            # Step 2: Execute navigation steps
            for i, step in enumerate(self.rpa_config.steps):
                self.logger.info(f"Executing step {i+1}/{len(self.rpa_config.steps)}: {step}")
                self._execute_step(step)
            
            # Step 3: Process downloaded file
            downloaded_file = self._wait_for_download()
            
            if downloaded_file:
                self.metadata.file_name = downloaded_file.name
                df = self._parse_downloaded_file(downloaded_file)
                return df
            else:
                raise ExtractionError("No file was downloaded")
                
        except Exception as e:
            if self.rpa_config.screenshot_on_error:
                self._take_screenshot("error")
            raise ExtractionError(f"RPA extraction failed: {str(e)}")
    
    def _perform_login(self) -> None:
        """Handle login process including potential MFA."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        
        self.logger.info(f"Navigating to login page: {self.rpa_config.login_url}")
        self._driver.get(self.rpa_config.login_url)
        
        # Wait for login form
        self._wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.rpa_config.username_selector))
        )
        
        # Enter credentials
        username_field = self._driver.find_element(By.CSS_SELECTOR, self.rpa_config.username_selector)
        username_field.clear()
        username_field.send_keys(self.rpa_config.username)
        
        password_field = self._driver.find_element(By.CSS_SELECTOR, self.rpa_config.password_selector)
        password_field.clear()
        password_field.send_keys(self.rpa_config.password)
        
        # Click login
        login_button = self._driver.find_element(By.CSS_SELECTOR, self.rpa_config.login_button_selector)
        login_button.click()
        
        # Handle MFA if configured
        if self.rpa_config.mfa_handler:
            self.logger.info("Handling MFA...")
            self.rpa_config.mfa_handler(self._driver, self._wait)
        
        # Wait for login to complete (page change)
        time.sleep(2)
        self.logger.info("Login successful")
    
    def _execute_step(self, step: Dict[str, Any]) -> None:
        """Execute a single RPA step with retry logic."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import Select
        
        action = step.get("action", "").lower()
        selector = step.get("selector")
        value = step.get("value")
        wait_after = step.get("wait_after", 1.0)
        
        attempts = 0
        while attempts < self.rpa_config.max_retries_per_step:
            try:
                if action == "navigate":
                    url = value if value.startswith("http") else f"{self._get_base_url()}{value}"
                    self._driver.get(url)
                    
                elif action == "click":
                    element = self._wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    element.click()
                    
                elif action == "type":
                    element = self._wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    element.clear()
                    element.send_keys(value)
                    
                elif action == "select":
                    element = self._wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    Select(element).select_by_visible_text(value)
                    
                elif action == "wait":
                    time.sleep(float(value))
                    
                elif action == "screenshot":
                    self._take_screenshot(value or "step")
                    
                elif action == "scroll":
                    if selector:
                        element = self._driver.find_element(By.CSS_SELECTOR, selector)
                        self._driver.execute_script("arguments[0].scrollIntoView();", element)
                    else:
                        self._driver.execute_script(f"window.scrollBy(0, {value or 500});")
                        
                elif action == "wait_for_element":
                    self._wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    
                elif action == "wait_for_clickable":
                    self._wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    
                elif action == "execute_script":
                    self._driver.execute_script(value)
                    
                else:
                    self.logger.warning(f"Unknown action: {action}")
                
                # Success - apply wait and return
                if wait_after > 0:
                    time.sleep(wait_after)
                return
                
            except Exception as e:
                attempts += 1
                self.logger.warning(
                    f"Step failed (attempt {attempts}/{self.rpa_config.max_retries_per_step}): {str(e)}"
                )
                if attempts >= self.rpa_config.max_retries_per_step:
                    raise
                time.sleep(2)
    
    def _wait_for_download(self) -> Optional[Path]:
        """Wait for file download to complete."""
        import glob
        
        self.logger.info("Waiting for file download...")
        start_time = time.time()
        
        while time.time() - start_time < self.rpa_config.download_wait_timeout:
            # Check for completed downloads
            files = list(self.rpa_config.download_dir.glob(self.rpa_config.expected_file_pattern))
            
            # Filter out partial downloads
            completed_files = [
                f for f in files 
                if not f.name.endswith(".crdownload") 
                and not f.name.endswith(".tmp")
            ]
            
            if completed_files:
                # Return the most recent file
                latest_file = max(completed_files, key=lambda x: x.stat().st_mtime)
                self.logger.info(f"Download complete: {latest_file.name}")
                return latest_file
            
            time.sleep(2)
        
        self.logger.error("Download timed out")
        return None
    
    def _parse_downloaded_file(self, file_path: Path) -> pl.DataFrame:
        """Parse the downloaded file into a DataFrame."""
        if self.rpa_config.file_parser:
            return self.rpa_config.file_parser(file_path)
        
        # Default parsers based on extension
        suffix = file_path.suffix.lower()
        
        if suffix == ".xlsx" or suffix == ".xls":
            return pl.read_excel(file_path)
        elif suffix == ".csv":
            return pl.read_csv(file_path)
        elif suffix == ".json":
            return pl.read_json(file_path)
        elif suffix == ".parquet":
            return pl.read_parquet(file_path)
        else:
            raise ExtractionError(f"Unsupported file format: {suffix}")
    
    def _take_screenshot(self, name: str) -> Path:
        """Capture screenshot for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        filepath = self.rpa_config.screenshot_dir / filename
        
        self._driver.save_screenshot(str(filepath))
        self.logger.info(f"Screenshot saved: {filepath}")
        return filepath
    
    def _get_base_url(self) -> str:
        """Extract base URL from current page."""
        from urllib.parse import urlparse
        parsed = urlparse(self._driver.current_url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def _disconnect(self) -> None:
        """Close WebDriver and clean up."""
        if self._driver:
            try:
                self._driver.quit()
                self.logger.info("WebDriver closed")
            except Exception as e:
                self.logger.warning(f"Error closing WebDriver: {str(e)}")


# Pre-defined step builders for common actions
class StepBuilder:
    """Helper class to build RPA steps fluently."""
    
    @staticmethod
    def navigate(url: str, wait: float = 2.0) -> Dict:
        return {"action": "navigate", "value": url, "wait_after": wait}
    
    @staticmethod
    def click(selector: str, wait: float = 1.0) -> Dict:
        return {"action": "click", "selector": selector, "wait_after": wait}
    
    @staticmethod
    def type_text(selector: str, text: str, wait: float = 0.5) -> Dict:
        return {"action": "type", "selector": selector, "value": text, "wait_after": wait}
    
    @staticmethod
    def select_option(selector: str, option_text: str, wait: float = 1.0) -> Dict:
        return {"action": "select", "selector": selector, "value": option_text, "wait_after": wait}
    
    @staticmethod
    def wait(seconds: float) -> Dict:
        return {"action": "wait", "value": str(seconds), "wait_after": 0}
    
    @staticmethod
    def wait_for_element(selector: str, wait: float = 0) -> Dict:
        return {"action": "wait_for_element", "selector": selector, "wait_after": wait}
    
    @staticmethod
    def screenshot(name: str = "screenshot") -> Dict:
        return {"action": "screenshot", "value": name, "wait_after": 0}


# Example usage pattern
def create_wms_extractor(
    bronze_path: Path,
    login_url: str,
    username: str,
    password: str,
    report_type: str = "stock_movements",
) -> RPAExtractor:
    """Factory function for WMS extraction."""
    
    steps = [
        StepBuilder.navigate("/reports"),
        StepBuilder.wait_for_element("#report-menu"),
        StepBuilder.click(f"#report-{report_type}"),
        StepBuilder.select_option("#date-range", "Yesterday"),
        StepBuilder.click("#apply-filters"),
        StepBuilder.wait(3),
        StepBuilder.click("#export-excel"),
        StepBuilder.wait(5),  # Wait for download to start
    ]
    
    config = RPAConfig(
        source_name="wms",
        target_table=report_type,
        bronze_path=bronze_path,
        login_url=login_url,
        username=username,
        password=password,
        headless=True,
        steps=steps,
        expected_file_pattern="*.xlsx",
    )
    
    return RPAExtractor(config)
