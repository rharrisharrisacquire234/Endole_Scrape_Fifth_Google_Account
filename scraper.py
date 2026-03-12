import os
import asyncio
import time
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()
EMAIL = os.getenv("ENDOLE_EMAIL")
PASSWORD = os.getenv("ENDOLE_PASSWORD")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Authenticate Google Sheets
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("Company")

# Get full sheet data
all_values = sheet.get_all_values()
headers = all_values[0]
rows = all_values[1:]

# Ensure 'Turnover' and 'Employee Size' columns exist
if "Turnover" not in headers:
    headers.append("Turnover")
    for row in rows:
        row.append("")
if "Employee Size" not in headers:
    headers.append("Employee Size")
    for row in rows:
        row.append("")

# Update headers if changed
sheet.update(values=[headers], range_name="A1")

# Get column indexes
reg_num_idx = headers.index("Companies House Regestration Number")
reg_name_idx = headers.index("Companies House Regestration Name")
turnover_idx = headers.index("Turnover")
employee_idx = headers.index("Employee Size")


def create_endole_slug(company_name):
    return (
        company_name.strip()
        .lower()
        .replace("&", "and")
        .replace(",", "")
        .replace(".", "")
        .replace("'", "")
        .replace("’", "")
        .replace(" ", "-")
    )


async def scrape_company_data(page, reg_number, company_slug):
    url = f"https://app.endole.co.uk/company/{reg_number}/{company_slug}"
    print(f"🔗 Visiting: {url}")
    await page.goto(url)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(5000)

    turnover, employees = "N/A", "N/A"

    try:
        fin_frame = next((f for f in page.frames if "tile=financials" in f.url), None)
        if fin_frame:
            # Turnover
            t_elem = fin_frame.locator("//div[contains(text(),'Turnover')]/following-sibling::div")
            if await t_elem.count() > 0:
                turnover = (await t_elem.first.text_content() or "").strip()

            # Employees
            e_elem = fin_frame.locator("//div[contains(text(),'Employees')]/following-sibling::div")
            if await e_elem.count() > 0:
                employees = (await e_elem.first.text_content() or "").strip()

    except Exception as e:
        print(f"⚠️ Error scraping financials: {e}")

    print(f"✅ Scraped → Turnover: {turnover}, Employees: {employees}")
    return turnover, employees


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Login to Endole
        print("🔐 Logging in to Endole...")
        await page.goto("https://app.endole.co.uk/login")
        await page.fill("input[name='email']", EMAIL)
        await page.fill("input[name='password']", PASSWORD)
        await page.click("button[type='submit']")
        await page.wait_for_load_state("networkidle")
        print("✅ Logged in successfully.\n")

        # Loop through rows
        for idx, row in enumerate(rows):
            try:
                reg_number = row[reg_num_idx].strip()
                reg_name = row[reg_name_idx].strip()
                turnover_val = row[turnover_idx].strip() if row[turnover_idx] else ""
                employee_val = row[employee_idx].strip() if row[employee_idx] else ""

                # Skip if no reg_number or name
                if not reg_number or not reg_name or reg_number.lower() == "nan":
                    print(f"⏭️ Skipping invalid row {idx + 2}")
                    continue

                # Skip if Turnover or Employee Size already filled
                if turnover_val or employee_val:
                    print(f"⏭️ Skipping row {idx + 2}, already has data")
                    continue

                slug = create_endole_slug(reg_name)
                turnover, emp_size = await scrape_company_data(page, reg_number, slug)

                # Update Google Sheet cells directly
                sheet.update_cell(idx + 2, turnover_idx + 1, turnover)
                sheet.update_cell(idx + 2, employee_idx + 1, emp_size)
                print(f"📝 Updated row {idx + 2} in sheet.")

                # ✅ Close the company tab inside Endole
                try:
                    close_btn = page.locator("div._close")
                    if await close_btn.count() > 0:
                        await close_btn.first.click()
                        print(f"🗂 Closed company tab for row {idx + 2}")
                        await page.wait_for_timeout(1000)  # small wait for UI to update
                except Exception as e:
                    print(f"⚠️ Could not close tab for row {idx + 2}: {e}")

                time.sleep(1)  # avoid Google API rate limit
            except Exception as e:
                print(f"❌ Error at row {idx + 2}: {e}")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
