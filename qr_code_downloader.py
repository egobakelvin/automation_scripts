import cx_Oracle
import os
import logging
from typing import List, Tuple, Optional
import time


def setup_logging(log_file: str):
    logging.basicConfig(
        filename=log_file,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def connect_to_oracle(username: str, password: str, dsn: str) -> cx_Oracle.Connection:
    try:
        conn = cx_Oracle.connect(username, password, dsn)
        print("Connected to Oracle DB")
 
        return conn
    except Exception as e:
        
        print(" failed to Connect to Oracle DB")
        raise


def get_total_rows(cursor: cx_Oracle.Cursor, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = cursor.fetchone()[0]
    print(f"Total rows to process: {total}")
    return total


def fetch_batch(
    cursor: cx_Oracle.Cursor,
    table_name: str,
    lower: int,
    upper: int
) -> List[Tuple[str, str, cx_Oracle.LOB]]:
    """
    Fetch a batch of records: (qr_file_name, plant_code, qr_code_blob)
    """
    query = f"""
        SELECT QR_FILE_NAME, PLANT_CODE, QR_CODE_BLOB
        FROM (
            SELECT QR_FILE_NAME, PLANT_CODE, QR_CODE_BLOB, ROWNUM AS rn
            FROM (
                SELECT QR_FILE_NAME, PLANT_CODE, QR_CODE_BLOB
                FROM {table_name}
            )
            WHERE ROWNUM <= :upper
        )
        WHERE rn > :lower
    """
    cursor.execute(query, upper=upper, lower=lower)
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} rows from offset {lower}")
    return rows



def save_qr_pdf(file_name: str, plant_code: str, blob: Optional[cx_Oracle.LOB], base_dir: str):
    if not blob:
        print(f"[WARN] No QR code blob for {file_name} in plant {plant_code}")
        return



def save_qr_pdf(file_name: str, plant_code: str, blob: Optional[cx_Oracle.LOB], base_dir: str):
    if not blob:
        print(f"[WARN] No QR code blob for {file_name} in plant {plant_code}")
        return


    safe_file_name = file_name.replace("/", "_").replace("\\", "_").strip()
    if not safe_file_name.lower().endswith(".pdf"):
        safe_file_name += ".pdf"

   
    plant_folder = os.path.join(base_dir, plant_code)
    os.makedirs(plant_folder, exist_ok=True)

  
    file_path = os.path.join(plant_folder, safe_file_name)

    try:
        print(f"[INFO] Saving: {file_path}")
        with open(file_path, "wb") as f:
            f.write(blob.read()) 
        print(f"[SUCCESS] Saved: {file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save {file_path}: {e}")

        print(f"[SUCCESS] Saved: {file_path}")
    except Exception as e:
        print(f"[ERROR] Writing file {file_path} failed: {e}")
    





def download_qr_codes_from_oracle(
    username: str,
    password: str,
    dsn: str,
    output_dir: str = "qr_codes",
    table_name: str = "ZSAP_FIX_ASSET",
    batch_size: int = 1000,
    log_file: str = "qr_export.log"
):
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(log_file)

    conn = connect_to_oracle(username, password, dsn)
    cursor = conn.cursor()

    total_rows = get_total_rows(cursor, table_name)

    offset = 0
    while offset < total_rows:
        upper = offset + batch_size
        lower = offset

        try:
            rows = fetch_batch(cursor, table_name, lower, upper)
            for file_name, plant_code, blob in rows:
                if file_name and plant_code:
                    save_qr_pdf(file_name, plant_code, blob, output_dir)
        except Exception as e:
            print(f"Batch {lower}-{upper} failed: {e}")

        offset += batch_size

    cursor.close()
    conn.close()
    print("QR Code Export completed successfully.")


if __name__ == "__main__":
    download_qr_codes_from_oracle(
        username="DUFIL",
        password="tERx3ELLFvw!7",
        dsn="revion-aws-eu-uk-ldb2.revion.com:15210/KT",
        output_dir=fr"C:\Users\egoba.kelvin\RPAdownloaded_qr_codes"
    )
