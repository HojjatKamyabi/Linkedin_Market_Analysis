from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from datetime import datetime
import re
import psycopg2


def connect_to_db():
    try:
        conn = psycopg2.connect(
            host='host.docker.internal',
            database='Linkedin_data',
            user='postgres',
            password='##password##',
            port='5432'
        )
        return conn
    
    except psycopg2.OperationalError as e:
        print(f" Connection to postgresql failed: {e}")
        return None 


                
def create_tables(conn):
    #Create tables if they don't exist
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            company TEXT NOT NULL,
            location TEXT NOT NULL,
            date_posted TEXT,
            description TEXT,
            job_link TEXT UNIQUE,
            scrape_date TIMESTAMP NOT NULL
        );
    """)
    
    conn.commit()
    cursor.close()
    print("Database tables created/verified")


def scrape_linkedin(job_title, location, conn):

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-notifications')

    driver = webdriver.Chrome(options=options)
    search_url = f"https://www.linkedin.com/jobs/search/?keywords={job_title.replace(' ', '%20')}&location={location.replace(' ', '%20')}&geoId=101282230&f_E=1%2C2&f_TPR=r604800&position=1&pageNum=0"

    # Navigate to the search page
    driver.get(search_url)
    time.sleep(3) 

    # Load more jobs 
    for i in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # Find job listings
        job_cards = driver.find_elements(By.CSS_SELECTOR, 'div.job-search-card')

        
    # Extract data from jobs
    cursor = conn.cursor()
    for i , job in enumerate(job_cards):
        try :
            # Find job link
            link_elem = job.find_element(By.CSS_SELECTOR, 'a.base-card__full-link')
            job_link = link_elem.get_attribute('href') if link_elem else None
            
            # Navigate to the job link
            driver.execute_script(f"window.open('{job_link}', '_blank');")
            time.sleep(3)
            driver.switch_to.window(driver.window_handles[1])
            # Get the job description
            desc_elem = driver.find_element(By.CSS_SELECTOR, 'div.show-more-less-html__markup')
            description_html = desc_elem.get_attribute('innerHTML')

            # parse the HTML
            soup = BeautifulSoup(description_html, 'html.parser')

            # Handle bullet points
            for li in soup.find_all('li'):
                li.insert_before("+ ")

            # Convert tags
            for br in soup.find_all(['br', 'p']):
                br.insert_after('\n')

            # Get the text
            description = soup.get_text()

            # Clean up the text
            description = re.sub(r'\n\s*\n', '\n\n', description)
            description = description.strip()
            
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

            job_html = job.get_attribute('outerHTML')
            job_soup = BeautifulSoup(job_html, 'html.parser')

            try:
                title_elem = job_soup.select_one('.base-search-card__title')
                title = title_elem.text.strip() if title_elem else "Not available"

                company_elem = job_soup.select_one('.base-search-card__subtitle')
                company = company_elem.text.strip() if company_elem else "Not available"

                location_elem = job_soup.select_one('.job-search-card__location')
                location = location_elem.text.strip() if location_elem else "Not available"

                date_elem = job_soup.select_one('time.job-search-card__listdate')
                date = date_elem.get('datetime') if date_elem else "Not available"

            except Exception as e:
                print(f"Error extracting job details: {e}")
            
            # Insert into database
            cursor.execute("""
                INSERT INTO jobs (title, company, location, date_posted, description, job_link, scrape_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_link) DO NOTHING
                RETURNING id
            """, (
                title,
                company,
                location,
                date,
                description,
                job_link,
                datetime.now()
            ))
            
            job_id = cursor.fetchone()
            conn.commit()

            print(f"Processed job {i+1}/{len(job_cards)}")   

        except Exception as e:
            print(f"Error processing job {i+1}: {str(e)}")
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            continue
    driver.quit()

    
def main():
    # Connect to PostgreSQL and set up tables
    conn = connect_to_db()
    if conn is None:
        print("Exiting: Could not connect to the database.")
        return
    create_tables(conn)
    
    # Lists
    locations = ["Germany"]
    job_titles = ["data analyst", "data engineer"]
    saved_files = []
    
    for job_title in job_titles:
        for location in locations:
            result_file = scrape_linkedin(job_title, location, conn)
            if result_file:
                saved_files.append(result_file)
    
    print("\n Scraping completed! \n")
    conn.close()

        
if __name__ == "__main__":
    main()