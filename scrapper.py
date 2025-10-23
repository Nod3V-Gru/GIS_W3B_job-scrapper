from playwright.sync_api import sync_playwright
import pandas as pd
import time
import mysql.connector
from datetime import datetime
import re
import urllib.parse

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
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
                job_url VARCHAR(500) NOT NULL,
                reviews_count INT DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create company_posts table for frequency analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                post_title VARCHAR(255),
                post_date DATE,
                post_url VARCHAR(500),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create related_jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS related_jobs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                main_job_id INT,
                related_job_title VARCHAR(255),
                related_company VARCHAR(255),
                related_location VARCHAR(255),
                related_url VARCHAR(500),
                FOREIGN KEY (main_job_id) REFERENCES jobs(id)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Database and tables created successfully")
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

def save_to_database(job_data, company_posts, related_jobs):
    """Save job data to MySQL database"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Insert main job data
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
        
        job_id = cursor.lastrowid
        
        # Insert company posts for frequency analysis
        if company_posts:
            company_query = """
                INSERT INTO company_posts (company_name, post_title, post_date, post_url)
                VALUES (%s, %s, %s, %s)
            """
            for post in company_posts:
                cursor.execute(company_query, (
                    post['company_name'],
                    post['post_title'],
                    post['post_date'],
                    post['post_url']
                ))
        
        # Insert related jobs
        if related_jobs:
            related_query = """
                INSERT INTO related_jobs (main_job_id, related_job_title, related_company, related_location, related_url)
                VALUES (%s, %s, %s, %s, %s)
            """
            for related_job in related_jobs:
                cursor.execute(related_query, (
                    job_id,
                    related_job['title'],
                    related_job['company'],
                    related_job['location'],
                    related_job['url']
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully saved job: {job_data['job_title']}")
        
    except mysql.connector.Error as e:
        print(f"Database save error: {e}")

def parse_date(date_text):
    """Parse date text from Indeed to actual date"""
    if not date_text:
        return None
    
    date_text = date_text.lower()
    today = datetime.now().date()
    
    if 'today' in date_text or 'just posted' in date_text:
        return today
    elif 'yesterday' in date_text:
        return today - pd.Timedelta(days=1)
    elif 'day' in date_text:
        days_ago = re.search(r'(\d+)\+? day', date_text)
        if days_ago:
            return today - pd.Timedelta(days=int(days_ago.group(1)))
    elif 'week' in date_text:
        weeks_ago = re.search(r'(\d+) week', date_text)
        if weeks_ago:
            return today - pd.Timedelta(weeks=int(weeks_ago.group(1)))
    elif 'month' in date_text:
        months_ago = re.search(r'(\d+) month', date_text)
        if months_ago:
            return today - pd.Timedelta(days=int(months_ago.group(1)) * 30)
    
    return None

def format_company_name_for_url(company_name):
    """Format company name for Indeed company page URL"""
    if not company_name:
        return ""
    
    # Remove common suffixes and special characters
    company_name = re.sub(r'[^\w\s-]', '', company_name)
    company_name = re.sub(r'\s+', '-', company_name.strip())
    company_name = company_name.lower()
    
    return company_name

def scrape_indeed(playwright):
    browser = playwright.chromium.launch_persistent_context(
        user_data_dir="C:\\playwright",
        channel="chrome",
        headless=False,
        no_viewport=True,
    )

    page = browser.new_page()
    page_count = 0
    all_jobs = []

    # Create database tables
    create_database()

    while page_count < 2:  # Adjust as needed
        print(f"SCRAPING PAGE {page_count + 1}")
        
        # Use the provided search link for first page, paginate for subsequent pages
        if page_count == 0:
            page.goto('https://www.indeed.com/jobs?q=GIS&l=&from=searchOnHP&vjk=380338992f3af91e')
        else:
            page.goto(f'https://www.indeed.com/jobs?q=GIS&start={page_count * 10}')
        
        time.sleep(5)
        
        # Get all job cards
        job_cards = page.locator('.cardOutline, .job_seen_beacon')
        
        for i in range(job_cards.count()):
            try:
                job_card = job_cards.nth(i)
                
                # Extract basic job info from listing
                job_data = {}
                
                # Get job title and URL
                title_element = job_card.locator('h2.jobTitle a, h2 a')
                if title_element.count() > 0:
                    job_data['job_title'] = title_element.inner_text().strip()
                    job_data['job_url'] = "https://www.indeed.com" + title_element.get_attribute('href')
                else:
                    continue
                
                # Get company name from search results page (previous page)
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
                
                # Now navigate to job detail page
                print(f"SCRAPING DETAILS PAGE: {job_data['job_title']}")
                page.goto(job_data['job_url'])
                time.sleep(3)
                
                # Get additional details from job page
                job_data['reviews_count'] = 0
                
                # Try to get reviews count
                try:
                    reviews_element = page.locator('[data-testid="reviews-count"], .reviews')
                    if reviews_element.count() > 0:
                        reviews_text = reviews_element.inner_text()
                        reviews_match = re.search(r'(\d+)', reviews_text)
                        if reviews_match:
                            job_data['reviews_count'] = int(reviews_match.group(1))
                except:
                    pass
                
                # Try to navigate to company page for more posts using company name from search results
                company_posts = []
                try:
                    if job_data['company_name']:
                        # Format company name for URL
                        formatted_company_name = format_company_name_for_url(job_data['company_name'])
                        if formatted_company_name:
                            company_url = f"https://www.indeed.com/cmp/{formatted_company_name}"
                            print(f"NAVIGATING TO COMPANY PAGE: {company_url}")
                            page.goto(company_url)
                            time.sleep(3)
                            
                            # Check if we're on a valid company page
                            if "company" in page.url or "cmp" in page.url:
                                # Scrape recent company posts
                                recent_posts = page.locator('.job, .jobCard, [data-testid="jobCard"]')
                                for j in range(min(5, recent_posts.count())):  # Get up to 5 recent posts
                                    post = recent_posts.nth(j)
                                    post_data = {}
                                    
                                    post_title = post.locator('h2 a, .jobTitle a, [data-testid="jobTitle"]')
                                    if post_title.count() > 0:
                                        post_data['post_title'] = post_title.inner_text().strip()
                                        post_url = post_title.get_attribute('href')
                                        if post_url:
                                            if post_url.startswith('/'):
                                                post_data['post_url'] = "https://www.indeed.com" + post_url
                                            else:
                                                post_data['post_url'] = post_url
                                        else:
                                            post_data['post_url'] = ""
                                        
                                        post_data['company_name'] = job_data['company_name']
                                        
                                        post_date = post.locator('.date, .datePosted, [data-testid="myJobsStateDate"]')
                                        if post_date.count() > 0:
                                            date_text = post_date.inner_text().strip()
                                            post_data['post_date'] = parse_date(date_text)
                                        else:
                                            post_data['post_date'] = None
                                        
                                        company_posts.append(post_data)
                            else:
                                print(f"Invalid company page for: {job_data['company_name']}")
                except Exception as e:
                    print(f"Error scraping company page: {e}")
                
                # Get related jobs
                related_jobs = []
                try:
                    # Navigate back to job page if we were on company page
                    page.goto(job_data['job_url'])
                    time.sleep(2)
                    
                    related_jobs_section = page.locator('[data-testid="relatedQuerySections"], .relatedJobs')
                    if related_jobs_section.count() > 0:
                        related_links = related_jobs_section.locator('a')
                        for j in range(min(5, related_links.count())):
                            related_job = {}
                            related_job['title'] = related_links.nth(j).inner_text().strip()
                            related_url = related_links.nth(j).get_attribute('href')
                            if related_url:
                                if related_url.startswith('/'):
                                    related_job['url'] = "https://www.indeed.com" + related_url
                                else:
                                    related_job['url'] = related_url
                            else:
                                related_job['url'] = ""
                            related_job['company'] = job_data['company_name']
                            related_job['location'] = job_data['location']
                            related_jobs.append(related_job)
                except Exception as e:
                    print(f"Error scraping related jobs: {e}")
                
                # Save to database
                save_to_database(job_data, company_posts, related_jobs)
                all_jobs.append(job_data)
                
                # Navigate back to search results
                print("RETURNING TO SEARCH RESULTS")
                if page_count == 0:
                    page.goto('https://www.indeed.com/jobs?q=GIS&l=&from=searchOnHP&vjk=380338992f3af91e')
                else:
                    page.goto(f'https://www.indeed.com/jobs?q=GIS&start={page_count * 10}')
                time.sleep(3)
                
            except Exception as e:
                print(f"Error processing job {i}: {e}")
                # Try to return to search results
                try:
                    if page_count == 0:
                        page.goto('https://www.indeed.com/jobs?q=GIS&l=&from=searchOnHP&vjk=380338992f3af91e')
                    else:
                        page.goto(f'https://www.indeed.com/jobs?q=GIS&start={page_count * 10}')
                    time.sleep(3)
                except:
                    pass
        
        page_count += 1

    browser.close()
    return all_jobs

with sync_playwright() as playwright:
    jobs = scrape_indeed(playwright)
    
    # Also save to Excel for backup
    df = pd.DataFrame(jobs)
    df.to_excel("gis_jobs.xlsx", index=True)
    print("Scraping completed and data saved to database and Excel file")
