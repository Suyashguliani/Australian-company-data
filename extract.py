import xml.etree.ElementTree as ET
import csv
import zipfile
import requests
import os

# URL and paths
zip_url = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip"
zip_path = "public_split_1_10.zip"
output_file = "abr_combined_extracted.csv"

# Files to process inside the ZIP
abr_files = [f"20250521_Public0{i}.xml" for i in range(1, 10)] + ["20250521_Public10.xml"]

# Step 1: Download ZIP if not already present
if not os.path.exists(zip_path):
    print(f"Downloading ZIP file from {zip_url} ...")
    response = requests.get(zip_url)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    print("Download complete.")

# Step 2: Extract and process XML files directly from ZIP
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([
        "ABN", "Entity Name", "Entity Type",
        "Entity Status", "Entity Start Date",
        "Entity State", "Entity Postcode"
    ])

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for file_name in abr_files:
            if file_name not in zipf.namelist():
                print(f"⚠️ Skipping {file_name} — not found in ZIP.")
                continue

            print(f"📂 Processing {file_name}...")

            with zipf.open(file_name) as xml_file:
                for event, elem in ET.iterparse(xml_file, events=("end",)):
                    if elem.tag == "ABR":
                        abn_elem = elem.find("ABN")
                        abn = abn_elem.text if abn_elem is not None else None
                        entity_status = abn_elem.get("status") if abn_elem is not None else None
                        start_date = abn_elem.get("ABNStatusFromDate") if abn_elem is not None else None

                        entity_type = elem.findtext("EntityType/EntityTypeText")
                        entity_name = elem.findtext("MainEntity/NonIndividualName/NonIndividualNameText")
                        state = elem.findtext("MainEntity/BusinessAddress/AddressDetails/State")
                        postcode = elem.findtext("MainEntity/BusinessAddress/AddressDetails/Postcode")

                        writer.writerow([
                            abn, entity_name, entity_type,
                            entity_status, start_date,
                            state, postcode
                        ])

                        elem.clear()

print(f"\n✅ Done! Combined output saved to: {output_file}")

