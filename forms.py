#forms.py
import requests
import json

response = requests.get(
    "https://data.sec.gov/submissions/CIK0000320193.json",
    headers={"User-Agent": "your-name your-email@example.com"}
)
data = response.json()

filings = data["filings"]["recent"]
forms = filings["form"]
accession_numbers = filings["accessionNumber"]
filing_dates = filings["filingDate"]
primary_docs = filings["primaryDocument"]

for i, form in enumerate(forms):
    if form == "8-K":
        print(accession_numbers[i], filing_dates[i], primary_docs[i])