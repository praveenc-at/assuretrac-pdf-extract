import streamlit as st
import anthropic
import re
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
# 1. Anthropic API setup (set your key as environment variable for security)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

load_dotenv()

# 2. Postgres connection setup (optional, if you want to save)
PG_CONN_PARAMS = dict(
    dbname="textile_db",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)
TABLE_NAME = "po_data"

# Utility functions

def extract_json_from_text(text):
    # Cleans and extracts JSON array
    cleaned = re.sub(r"```json|```|json", "", text, flags=re.IGNORECASE).strip()
    match = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if match:
        json_str = match.group(0)
    else:
        json_str = cleaned
    return json.loads(json_str)


def normalize_json_keys(json_data):
    # Lowercase all keys
    return [{k.lower(): v for k, v in row.items()} for row in json_data]


def write_json_to_postgres(json_data, table_name, conn):
    columns = list(json_data[0].keys())
    col_names = ",".join(columns)
    values = [[row.get(col) for col in columns] for row in json_data]
    insert_query = f'INSERT INTO {table_name} ({col_names}) VALUES %s'
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)
    conn.commit()


def read_table_to_excel(table_name, conn_params, excel_path):
    # Export table to Excel
    db_url = (
        f"postgresql://{conn_params['user']}:{conn_params['password']}"
        f"@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
    )
    engine = create_engine(db_url)
    df = pd.read_sql_query(f'SELECT * FROM {table_name}', engine)
    df.to_excel(excel_path, index=False)
    return excel_path

# Streamlit UI
st.set_page_config(page_title="Assuretrac PDF Extractor", layout="wide")
st.title("📄 Assuretrac PDF Extractor")

# File uploader and question input
uploaded_file = st.file_uploader("Upload your PDF", type=["pdf"])
question = st.text_input("Enter your extraction question(Explain detailed for complex extraction)", "")
process_btn = st.button("Process PDF")

if process_btn:
    if not uploaded_file:
        st.error("Please upload a PDF file.")
    elif not question.strip():
        st.error("Please enter a question.")
    else:
        with st.spinner("Extracting data..."):
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            file_upload = client.beta.files.upload(
                file=(uploaded_file.name, uploaded_file, "application/pdf")
            )
            prompt = (
                "Extract all relevant details from the input and answer the question: "
                f"{question}. Return the output as a JSON array of objects."
            )
            response = client.beta.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=20000,
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "document", "source": {"type": "file", "file_id": file_upload.id}}
                    ]}
                ],
                betas=["files-api-2025-04-14"],
            )
            raw_text = response.content[0].text
        try:
            data = extract_json_from_text(raw_text)
            data = normalize_json_keys(data)
            df = pd.DataFrame(data)
            st.success("Data extracted successfully!")
            st.dataframe(df)

            # # Optional: Save to Postgres
            # if st.checkbox("Save to Postgres?", key="save_pg"):
            #     conn = psycopg2.connect(**PG_CONN_PARAMS)
            #     write_json_to_postgres(data, TABLE_NAME, conn)
            #     conn.close()
            #     st.info("Data saved to Postgres.")

            # # Optional: Export to Excel
            # if st.checkbox("Export to Excel?", key="export_xl"):
            #     path = read_table_to_excel(TABLE_NAME, PG_CONN_PARAMS, "po_data.xlsx")
            #     st.success(f"Excel file saved: {path}")
        except Exception as e:
            st.error(f"Failed to parse JSON: {e}")

