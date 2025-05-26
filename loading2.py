import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Database connection parameters — update accordingly
DB_PARAMS = {
    "host": "localhost",
    "dbname": "your_db_name",
    "user": "your_username",
    "password": "your_password",
    "port": 5432
}

CSV_FILE_PATH = "abr_combined_extracted.csv"

def parse_date(date_str):
    # Adjust format as per your CSV, example 'YYYY-MM-DD' or 'DD/MM/YYYY'
    # Return None if date_str is empty or invalid
    if not date_str or date_str.strip() == "":
        return None
    try:
        # Try ISO format first:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            # Alternative format example (uncomment if needed)
            # return datetime.strptime(date_str, "%d/%m/%Y").date()
            return None
        except Exception:
            return None

def insert_companies_from_csv(csv_path):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        insert_query = """
        INSERT INTO companies (abn, entity_name, entity_type, entity_status, entity_start_date, state, postcode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (abn) DO UPDATE SET
            entity_name = EXCLUDED.entity_name,
            entity_type = EXCLUDED.entity_type,
            entity_status = EXCLUDED.entity_status,
            entity_start_date = EXCLUDED.entity_start_date,
            state = EXCLUDED.state,
            postcode = EXCLUDED.postcode;
        """

        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                abn = row.get('abn') or row.get('ABN') or row.get('Abn')
                entity_name = row.get('entity_name') or row.get('EntityName') or row.get('entity_name')
                entity_type = row.get('entity_type') or row.get('EntityType')
                entity_status = row.get('entity_status') or row.get('EntityStatus')
                entity_start_date = parse_date(row.get('entity_start_date') or row.get('EntityStartDate'))
                state = row.get('state') or row.get('State')
                postcode = row.get('postcode') or row.get('Postcode')

                # Basic validation for mandatory fields
                if not abn or not entity_name:
                    print(f"Skipping row with missing mandatory fields: {row}")
                    continue

                cur.execute(insert_query, (
                    abn.strip(),
                    entity_name.strip(),
                    entity_type.strip() if entity_type else None,
                    entity_status.strip() if entity_status else None,
                    entity_start_date,
                    state.strip() if state else None,
                    postcode.strip() if postcode else None
                ))

        conn.commit()
        print("CSV data inserted/updated successfully.")

    except Exception as e:
        print("Error:", e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_companies_from_csv(CSV_FILE_PATH)
