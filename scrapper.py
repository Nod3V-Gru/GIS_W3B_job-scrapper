from playwright.sync_api import sync_playwright
import pandas as pd
import time
import mysql.connector
from datetime import datetime, timedelta
import re

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'user_name',
    'password': 'your_password',
    'database': 'indeed_jobs'
}

def create_database():
    """Create database and tables if they don't exist"""
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        
        # Create jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_title VARCHAR(255) NOT NULL,
                company_name VARCHAR(255),
                location VARCHAR(255),
                date_posted DATE,
                salary_info TEXT,
                job_url VARCHAR(500) NOT NULL UNIQUE,
                reviews_count INT DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_job_url (job_url),
                INDEX idx_company_name (company_name),
                INDEX idx_location (location),
                INDEX idx_date_posted (date_posted)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Database and tables created successfully")
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

def is_job_exists(job_url):
   try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM jobs WHERE job_url = %s", (job_url,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result is not None
        
    except mysql.connector.Error as e:
        print(f"Error checking job existence: {e}")
        return False

def save_to_database(job_data):
    if is_job_exists(job_data['job_url']):
        print(f"Job already exists: {job_data['job_title']}")
        return False
        
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
       
        job_query = """
            INSERT INTO jobs (job_title, company_name, location, date_posted, salary_info, job_url, reviews_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(job_query, (
            job_data['job_title'],
            job_data['company_name'],
            job_data['location'],
            job_data['date_posted'],
            job_data['salary_info'],
            job_data['job_url'],
            job_data['reviews_count']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully saved job: {job_data['job_title']}")
        return True
        
    except mysql.connector.Error as e:
        print(f"Database save error: {e}")
        return False

def parse_date(date_text):
    """Parse date text from Indeed to actual date"""
    if not date_text:
        return None
    
    date_text = date_text.lower()
    today = datetime.now().date()
    
    if 'today' in date_text or 'just posted' in date_text:
        return today
    elif 'yesterday' in date_text:
        return today - timedelta(days=1)
    elif 'day' in date_text:
        days_ago = re.search(r'(\d+)\+? day', date_text)
        if days_ago:
            return today - timedelta(days=int(days_ago.group(1)))
    elif 'week' in date_text:
        weeks_ago = re.search(r'(\d+) week', date_text)
        if weeks_ago:
            return today - timedelta(weeks=int(weeks_ago.group(1)))
    elif 'month' in date_text:
        months_ago = re.search(r'(\d+) month', date_text)
        if months_ago:
            return today - timedelta(days=int(months_ago.group(1)) * 30)
    
    return None

def scrape_indeed():
    """Main scraping function"""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch_persistent_context(
            user_data_dir="C:\\playwright",
            channel="chrome",
            headless=False,
            no_viewport=True,
        )

        page = browser.new_page()
        page_count = 0
        all_jobs = []
        max_pages = 5  # Limit pages to avoid too many requests

        # Create database tables
        create_database()

        while page_count < max_pages:
            print(f"SCRAPING PAGE {page_count + 1}")
            
            # Construct search URL
            if page_count == 0:
                search_url = 'https://www.indeed.com/jobs?q=GIS&l='
            else:
                search_url = f'https://www.indeed.com/jobs?q=GIS&start={page_count * 10}'
            
            print(f"Navigating to: {search_url}")
            page.goto(search_url)
            time.sleep(5)
            
            # Get all job cards
            job_cards = page.locator('.cardOutline, .job_seen_beacon')
            job_count = job_cards.count()
            
            if job_count == 0:
                print("No job cards found. Possible CAPTCHA or blocking.")
                break
            
            print(f"Found {job_count} job cards on page {page_count + 1}")
            
            new_jobs_found = 0
            for i in range(job_count):
                try:
                    job_card = job_cards.nth(i)
                    
                    # Extract basic job info from listing
                    job_data = {}
                    
                    # Get job title and URL
                    title_element = job_card.locator('h2.jobTitle a, h2 a')
                    if title_element.count() > 0:
                        job_data['job_title'] = title_element.inner_text().strip()
                        relative_url = title_element.get_attribute('href')
                        job_data['job_url'] = "https://www.indeed.com" + relative_url if relative_url.startswith('/') else relative_url
                    else:
                        continue
                    
                    # Skip if job already exists
                    if is_job_exists(job_data['job_url']):
                        print(f"Skipping existing job: {job_data['job_title']}")
                        continue
                    
                    # Get company name
                    company_element = job_card.locator('[data-testid="company-name"], .companyName')
                    if company_element.count() > 0:
                        job_data['company_name'] = company_element.inner_text().strip()
                    else:
                        job_data['company_name'] = ""
                    
                    # Get location
                    location_element = job_card.locator('[data-testid="text-location"], .companyLocation')
                    if location_element.count() > 0:
                        job_data['location'] = location_element.inner_text().strip()
                    else:
                        job_data['location'] = ""
                    
                    # Get date posted
                    date_element = job_card.locator('.date, [data-testid="myJobsStateDate"]')
                    if date_element.count() > 0:
                        date_text = date_element.inner_text().strip()
                        job_data['date_posted'] = parse_date(date_text)
                    else:
                        job_data['date_posted'] = None
                    
                    # Get salary info if available
                    salary_element = job_card.locator('.salary-snippet-container, .salaryOnly, .estimated-salary')
                    if salary_element.count() > 0:
                        job_data['salary_info'] = salary_element.inner_text().strip()
                    else:
                        job_data['salary_info'] = ""
                    
                    # Get reviews count from search results if available
                    job_data['reviews_count'] = 0
                    reviews_element = job_card.locator('[data-testid="company-rating"], .ratingNumber')
                    if reviews_element.count() > 0:
                        reviews_text = reviews_element.inner_text()
                        reviews_match = re.search(r'(\d+)', reviews_text)
                        if reviews_match:
                            job_data['reviews_count'] = int(reviews_match.group(1))
                    
                    # Save to database
                    if save_to_database(job_data):
                        new_jobs_found += 1
                        all_jobs.append(job_data)
                    
                except Exception as e:
                    print(f"Error processing job {i}: {e}")
                    continue
            
            print(f"Page {page_count + 1}: Found {new_jobs_found} new jobs")
            
            # Stop if no new jobs found on current page
            if new_jobs_found == 0 and page_count > 0:
                print("No new jobs found on current page. Stopping pagination.")
                break
                
            page_count += 1
            time.sleep(2)  # Be respectful with requests

        browser.close()
        
        # Save to Excel for backup
        if all_jobs:
            df = pd.DataFrame(all_jobs)
            df.to_excel("gis_jobs.xlsx", index=False)
            print(f"Scraping completed. Found {len(all_jobs)} new jobs. Data saved to database and Excel file.")
        else:
            print("No new jobs found.")
        
        return all_jobs

if __name__ == "__main__":
    scrape_indeed()
