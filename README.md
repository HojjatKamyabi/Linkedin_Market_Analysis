![alt text](https://github.com/HojjatKamyabi/Linkedin_Market_Analysis/blob/main/visualization/Dashboard_PowerBI.jpg?raw=true)

# LinkedIn Job Scraper & Skill Extractor

A data pipeline project that:
- Scrapes job postings from LinkedIn
- Extracts skills
- Stores data in PostgreSQL
- Orchestrated via Apache Airflow
- Visualized in a dashboard

This project automatically scrapes LinkedIn job postings (for roles like Data Analyst and Data Engineer), extracts job descriptions, identifies required skills using AI, stores the data in a PostgreSQL database, and visualizes key insights using Power BI.  
It is fully containerized with Docker, managed with Airflow, and written in Python.

---

##  Prerequisites

To run this project successfully, make sure you have the following:

### Software & Tools
- Python 3.10+
- Docker & Docker Compose
- Chrome & ChromeDriver (for headless Selenium usage)
- PostgreSQL 15+
- Power BI (for visualization, optional)

### Python Libraries
- Selenium
- BeautifulSoup4
- psycopg2
- fuzzywuzzy
- pandas
- SQLAlchemy
- airflow
  
(All dependencies in `requirements.txt`)

### System Requirements
- Ability to run Docker containers
- Basic familiarity with Git, Python, and SQL
- Network access to LinkedIn (requires login)
- Enough memory to run Airflow, PostgreSQL, and scraping containers

---

##  Tech Stack

- **Python** (Web scraping, data cleaning, and skill extraction)
- **PostgreSQL** (Database to store job data)
- **Docker & Docker Compose** (Containerization of the whole project)
- **Apache Airflow** (Workflow management and scheduling)
- **Power BI** (Dashboard for showcasing insights)

---

##  Features

- Scrapes LinkedIn job postings using a Python script
- Extracts key job data like title, company, location, and full job descriptions
- Identifies required skills using keyword extraction methods
- Stores structured data in a PostgreSQL DB
- Automates the whole process with Airflow
- Displays the data in Power BI dashboards

---

## Setup Instructions

```bash
# Clone the repo
git clone https://github.com/HojjatKamyabi/Linkedin_Market_Analysis.git
cd Linkedin_Market_Analysis

# Spin up containers
docker compose up --build
```
## Check out full project description on my [personal website!](https://hojjatkamyabi.github.io/job_scraper.html)
