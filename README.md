# GIS_W3B_job-scrapper
Automated crawler for GIS jobs in indeed.com using Python playwright, mysql-connector-python, pandas, openpyxl libraries.
Features
Multi-website scraping: Supports multiple service providers like LikesOutlet, GodOfPanel, Followiz, and SMMBind.
MySQL Integration: Stores scraped data in a structured MySQL database for analysis and usage.
CSV Export: Combines and exports scraped data to CSV format.

Prerequisites:
Python 3.7 or higher

pandas 2.3 or higher

openpyxl 3.1 or higher

mysql-connector-python==8.0.33


Setup
1. Clone the repository
   git clone https://github.com/Nod3V-Gru/GIS_W3B_job-scrapper

2. Install Dependencies
   Install the required Python packages using the command below:

   pip install -r requirements.txt

3. Configure MySQL Database
   Create a database named indeed_jobs.

   Update the connect_to_database function in the Python script with your MySQL credentials.
