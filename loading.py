import csv
import psycopg2

DB_CONFIG = {
    'dbname': 'my_database',
    'user': 'postgres',
    'password': '<Yoour Password>',
    'host': 'localhost',
    'port': '5432'
}

CSV_FILE = 'australian_companies.csv'

def insert_websites(csv_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        for row in reader:
            
            if len(row) < 2:
                continue  # Skip incomplete rows
            raw_name = row[0]
            website = row[1]

            try:
                print(f"Inserting: {website}, {raw_name}")
                cursor.execute("""
                    INSERT INTO websites (website_url, extracted_company_name)
                    VALUES (%s, %s)
                    ON CONFLICT (website_url) DO NOTHING
                """, (website, raw_name))
                count += 1

            except Exception as e:
                print(f"Error inserting row {row}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {count} rows successfully.")

if __name__ == "__main__":
    insert_websites(CSV_FILE)
