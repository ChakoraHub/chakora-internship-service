import os
import time
import pytest
import requests


def get_base_url():
    return os.getenv("INTERNSHIP_BASE_URL", "http://127.0.0.1:8080").rstrip("/")


def should_expect_maintenance():
    return os.getenv("EXPECT_MAINTENANCE_NOTICE", "false").strip().lower() in ("true", "1", "yes")


def test_internship_page_responsive():
    """Verify that the /internships page is accessible and returns HTTP 200."""
    base_url = get_base_url()
    url = f"{base_url}/internships"
    resp = requests.get(url, timeout=15)
    assert resp.status_code == 200, f"Expected HTTP 200 from {url}, got {resp.status_code}"
    assert "Internship" in resp.text, "Response HTML does not contain 'Internship'"


def test_internship_maintenance_status_endpoint():
    """Verify that the maintenance status endpoint is operational and returns valid JSON."""
    base_url = get_base_url()
    url = f"{base_url}/internship/maintenance/status"
    resp = requests.get(url, timeout=10)
    assert resp.status_code == 200, f"Expected HTTP 200 from {url}, got {resp.status_code}"
    
    data = resp.json()
    assert "maintenance_mode" in data, "Key 'maintenance_mode' missing from status response"
    
    if should_expect_maintenance():
        assert data.get("maintenance_mode") is True, f"Expected maintenance_mode=True, got {data.get('maintenance_mode')}"
    print(f"✅ Maintenance status check passed | maintenance_mode={data.get('maintenance_mode')}")


def test_selenium_internship_page_render():
    """Verify page renders properly via headless Chrome."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        pytest.skip("Selenium not installed; skipping browser rendering test")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as exc:
        pytest.skip(f"Chrome WebDriver not available: {exc}")

    base_url = get_base_url()
    try:
        driver.get(f"{base_url}/internships")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        assert "Internship" in driver.title or "ChakoraHub" in driver.page_source

        if should_expect_maintenance():
            # Wait for client-side maintenance status fetch to resolve
            time.sleep(2)
            has_notice = (
                len(driver.find_elements(By.ID, "internship-maintenance-notice")) > 0
                or "Maintenance in Progress" in driver.page_source
                or "Scheduled Maintenance" in driver.page_source
            )
            assert has_notice, "Expected maintenance notice to be present in rendered DOM"
            print("✅ Selenium verified maintenance notice rendered on page")
        else:
            print("✅ Selenium verified internship page loaded successfully")

    finally:
        driver.quit()
