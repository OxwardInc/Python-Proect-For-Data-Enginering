import pandas as pd
import requests
from io import StringIO

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

response = requests.get(url)
response.raise_for_status()

tables = pd.read_html(StringIO(response.text))

print(f"\nFound {len(tables)} tables on the page\n")

# Loop through all tables and print their head
for i, table in enumerate(tables):
    print(f"========== TABLE {i+1} ==========")
    print(table.head())
    print("\n")