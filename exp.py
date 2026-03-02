import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta



load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    host=os.getenv("DB_HOST")
)

cur = conn.cursor()

cur.execute(
    "SELECT * FROM transactions ORDER BY created_at DESC;"
)


year = timedelta(days=365)
day=timedelta(days=1)
nowDate=datetime.now().date()
startDate=nowDate-year


print(nowDate,startDate)

print(nowDate>startDate)

