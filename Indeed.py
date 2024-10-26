import pandas as pd
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import re
import time

def get_job_urls(base_url, start_page, num_pages=2):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive",
    }

    job_urls = []

    for page in range(start_page, start_page + num_pages * 10, 10):
        url = f"{base_url}&start={page}"
        request_site = Request(url, headers=headers)
        try:
            html = urlopen(request_site).read()
            soup = BeautifulSoup(html, 'html.parser')
            job_cards = soup.find_all('a', class_='jcs-JobTitle css-jspxzf eu4oa1w0')

            for card in job_cards:
                job_url = card.get('href')
                if job_url:
                    if 'clk' in job_url:
                        if not job_url.startswith('http'):
                            job_url = f'https://in.indeed.com{job_url}'
                        job_urls.append(job_url)

            time.sleep(2)
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")

    return job_urls

def scrape_job_details(job_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive",
    }

    try:
        request_site = Request(job_url, headers=headers)
        html = urlopen(request_site).read()
        soup = BeautifulSoup(html, 'html.parser')

        job_title = soup.find('h1', {"class": "jobsearch-JobInfoHeader-title css-1b4cr5z e1tiznh50"})
        company_name = soup.find("span", {"class": "css-1saizt3 e1wnkr790"})
        company_location = soup.find("div", {"class": "css-waniwe eu4oa1w0"})
        salary = soup.find("div", {"id": "salaryInfoAndJobType"})

        job_title = job_title.text.strip() if job_title else "Job Title not found"
        company_name = company_name.text.strip() if company_name else "Company Name not found"
        company_location = company_location.text.strip() if company_location else "Company Location not found"
        salary = salary.get_text(strip=True) if salary else "Salary not mentioned"

        experience_text = ""
        for text in soup.stripped_strings:
            match = re.search(r'(\d+[\+]*[-]*\d*)\s+(years|yrs)', text, re.IGNORECASE)
            if match:
                experience_text = match.group()
                break

        page_text = soup.get_text()
        job_description_pattern = re.compile(r'(Full job description|Job details)[\s\S]*?(?=\n\n|\n\s*\n|$)', re.IGNORECASE)
        skills_pattern = re.compile(r'(Skills)[\s\S]*?(?=\n\n|\n\s*\n|$)', re.IGNORECASE)

        job_description_match = job_description_pattern.search(page_text)
        job_description = job_description_match.group().strip() if job_description_match else "Job Description not found"

        skills_match = skills_pattern.search(page_text)
        skills_text = skills_match.group().strip() if skills_match else "Skills information not found"

        return {
            "Job Title": job_title,
            "Company Name": company_name,
            "Company Location": company_location,
            "Salary": salary,
            "Experience Required": experience_text,
            "Job Description": job_description,
            "Skills and Related Content": skills_text
        }
    except Exception as e:
        print(f"Error scraping job details from {job_url}: {e}")
        return None

def main():
    base_url = "https://in.indeed.com/jobs?q=Data+Science%2Canalyst%2Cflutter%2Cfullstack%2Cpython%2CDigital+Marketing%2Cui+ux%2Cbuisnuess+analyst%2CMern&l=India&from=searchOnHP&vjk=c6d35cd0d770306e&advn=2907042266095773"
    start_page = 0
    pages_per_batch = 2
    total_batches = 34
    all_job_details = []

    for batch in range(total_batches):
        print(f"Processing batch {batch + 1}...")
        job_urls = get_job_urls(base_url, start_page, num_pages=pages_per_batch)
        start_page += pages_per_batch * 10

        for job_url in job_urls:
            job_details = scrape_job_details(job_url)
            if job_details:
                all_job_details.append(job_details)

        df = pd.DataFrame(all_job_details)
        df.to_csv('job_market2.csv', mode='a', index=False, header=not batch)

        all_job_details = []

        print("Waiting for 10 minutes...")
        time.sleep(605)

if __name__ == "__main__":
    main()
