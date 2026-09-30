import os

import pytest

# pyrefly: ignore [missing-import]
from selenium import webdriver

# pyrefly: ignore [missing-import]
from selenium.webdriver.common.by import By

# pyrefly: ignore [missing-import]
from selenium.webdriver.support.ui import WebDriverWait

# pyrefly: ignore [missing-import]
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv(
    "INTERNSHIP_BASE_URL",
    "https://www.chakorahub.com"
).rstrip("/")

INTERNSHIP_URL = f"{BASE_URL}/internships"
WAIT = 25


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1000")

    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        browser.quit()


def wait_loaded(browser):
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script(
            "return document.readyState"
        ) == "complete"
    )


def test_internship_page_opens(driver):
    """Verify the public Internship page loads."""

    driver.get(INTERNSHIP_URL)
    wait_loaded(driver)

    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, ".header-section h1")
        )
    )

    assert "ChakoraHub Internship Program" in heading.text

    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    form = driver.find_element(By.ID, "internshipForm")

    # The application form remains visible in both modes.
    assert form.is_displayed()

    if expected:
        notice = WebDriverWait(driver, WAIT).until(
            EC.visibility_of_element_located(
                (By.ID, "internship-maintenance-notice")
            )
        )

        assert "Scheduled maintenance notice" in notice.text


def test_maintenance_notice_visible_when_expected(driver):
    """Check the maintenance notice when expected."""

    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    if not expected:
        pytest.skip(
            "Maintenance notice is not expected for this run."
        )

    driver.get(INTERNSHIP_URL)
    wait_loaded(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "internship-maintenance-notice")
        )
    )

    assert "Scheduled maintenance notice" in notice.text

    # Form should remain visible during maintenance.
    form = driver.find_element(By.ID, "internshipForm")
    assert form.is_displayed()