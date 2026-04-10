"""Capture full-page screenshots of all Streamlit dashboard pages.

Streamlit renders content inside an internal scrollable container,
so we measure its scroll height and expand the viewport to fit,
giving us a true top-to-bottom capture.
"""

from playwright.sync_api import sync_playwright

BASE_URL = "http://localhost:8501"
OUTPUT_DIR = "docs/screenshots"

PAGES = {
    "live_data": "Live Data",
    "overview": "Market Overview",
    "stock_detail": "Stock Detail",
    "sector_analysis": "Sector Analysis",
}

WIDTH = 1400


def _capture_full_page(page, path: str) -> None:
    """Expand viewport to the full content height, then screenshot."""
    # Streamlit's scrollable container
    height = page.evaluate("""() => {
        const selectors = [
            'section.main .block-container',
            '[data-testid="stAppViewBlockContainer"]',
            'section.main',
            '[data-testid="stAppViewContainer"]',
        ];
        for (const sel of selectors) {
            const el = document.querySelector(sel);
            if (el) return el.scrollHeight;
        }
        return document.body.scrollHeight;
    }""")
    # Add padding for header/sidebar chrome
    full_height = height + 200
    page.set_viewport_size({"width": WIDTH, "height": full_height})
    page.wait_for_timeout(1500)
    page.screenshot(path=path, full_page=True)
    # Reset viewport for next page
    page.set_viewport_size({"width": WIDTH, "height": 900})


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": 900})

        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        for filename, nav_label in PAGES.items():
            radio = page.get_by_text(nav_label, exact=True)
            if radio.count() > 0:
                radio.first.click()
                page.wait_for_timeout(6000)
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(2000)

            path = f"{OUTPUT_DIR}/{filename}.png"
            _capture_full_page(page, path)
            print(f"Saved: {path}")

        browser.close()
        print("All screenshots captured.")


if __name__ == "__main__":
    main()
