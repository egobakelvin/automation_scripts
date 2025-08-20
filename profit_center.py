import os
import pandas as pd
from datetime import datetime

def read_excel_file(file_path: str) -> dict:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_excel(file_path, sheet_name=None)


def clean_code(val):
    try:
        return str(int(float(val))).strip() if isinstance(val, (int, float)) else str(val).strip()
    except:
        return ""


     



def process_sheet(prev_sheet: pd.DataFrame, curr_sheet: pd.DataFrame) -> pd.DataFrame:
    curr_sheet = curr_sheet.copy()

    def safe_float(val):
        try:
            return float(val) if pd.notna(val) else 0.0
        except Exception:
            return 0.0

    def is_valid_code(rm_code, fg_code):
        rm = str(rm_code).strip()
        fg = str(fg_code).strip()
        return not (rm == "" and fg in ["0", "0.0"])

   
    for col in ["RM_MatCode", "FG_MatCode"]:
        if col in prev_sheet.columns:
            prev_sheet[col] = prev_sheet[col].apply(clean_code)
        if col in curr_sheet.columns:
            curr_sheet[col] = curr_sheet[col].apply(clean_code)

    
    numeric_cols = [
        "RM-Op_Stock", "Val.Diff_OpStock",
        "FG-Op_Stock", "Val.Diff_FG_ClsStock",
        "FG_Val.Diff_OpStock", "ClsStock_Qty", "Val.Diff_ClsStock"
    ]
    for col in numeric_cols:
        if col in curr_sheet.columns:
            curr_sheet[col] = curr_sheet[col].apply(safe_float)

    
    curr_sheet["Prod_process_reference"] = curr_sheet["RM_MatCode"].fillna("").astype(str) + curr_sheet["FG_MatCode"].fillna("").astype(str)
    prev_sheet["Prod_process_reference"] = prev_sheet["RM_MatCode"].fillna("").astype(str) + prev_sheet["FG_MatCode"].fillna("").astype(str)

    
    curr_refs = set(curr_sheet["Prod_process_reference"])

    
    for index, row in curr_sheet.iterrows():
        rm_code = row.get("RM_MatCode", "")
        fg_code = row.get("FG_MatCode", "")
        # skip if RM == "" and FG == 0
        if not is_valid_code(rm_code, fg_code):
            continue  

        ref = row["Prod_process_reference"]
        if not ref.strip():
            continue

        match = prev_sheet[prev_sheet["Prod_process_reference"] == ref]
        if not match.empty:
            prev_row = match.iloc[0]
            curr_sheet.at[index, "RM-Op_Stock"] = safe_float(prev_row.get("ClsStock_Qty"))
            curr_sheet.at[index, "Val.Diff_OpStock"] = safe_float(prev_row.get("Val.Diff_ClsStock"))
            curr_sheet.at[index, "FG-Op_Stock"] = safe_float(prev_row.get("FG_ClsStock_Qty"))
            curr_sheet.at[index, "FG_Val.Diff_OpStock"] = safe_float(prev_row.get("Val.Diff_FG_ClsStock"))

   
    for _, row in prev_sheet.iterrows():
        rm_code = row.get("RM_MatCode", "")
        fg_code = row.get("FG_MatCode", "")
        if not is_valid_code(rm_code, fg_code):
            continue

        ref = row.get("Prod_process_reference", "").strip()
        if not ref or ref in curr_refs:
            continue

        new_row = {
            "RM_MatCode": rm_code,
            "FG_MatCode": fg_code,
            "RM-Op_Stock": safe_float(row.get("ClsStock_Qty")),
            "Val.Diff_OpStock": safe_float(row.get("Val.Diff_ClsStock")),
            "FG-Op_Stock": safe_float(row.get("FG_ClsStock_Qty")),
            "FG_Val.Diff_OpStock": safe_float(row.get("Val.Diff_FG_ClsStock")),
            "Prod_process_reference": ref
        }
        curr_sheet = pd.concat([curr_sheet, pd.DataFrame([new_row])], ignore_index=True)

    curr_sheet["Prod_Process_No"] = curr_sheet["Prod_process_reference"]
        
    if "Prod_process_reference" in prev_sheet.columns:
        prev_sheet.drop(columns=["Prod_process_reference"], inplace=True)
    if "Prod_process_reference" in curr_sheet.columns:
        curr_sheet.drop(columns=["Prod_process_reference"], inplace=True)

    return curr_sheet




def save_updated_excel(file_path: str, data: dict):
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        for sheet_name, sheet_data in data.items():
            sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"\nFile saved: {file_path}")

def main():
    now = datetime.now()
    adjusted_month = now.month - 1
    adjusted_year = now.year

    if adjusted_month == 0:  
        adjusted_month = 12
        adjusted_year -= 1

   
    previous_month = adjusted_month - 1
    previous_year = adjusted_year

    if previous_month == 0:  
        previous_month = 12
        previous_year -= 1

    current_file_path= fr"C:\Users\egoba.kelvin\RPA\1810_04_2025.xlsx"
    previous_file_path = fr"C:\Users\egoba.kelvin\RPA\1810_03_2025.xlsx"
    # Dynamic paths
    #current_file_path = fr"C:\RPA\SAP\Profit_Centre\Output\{adjusted_year}\{adjusted_month:02d}\1810_{adjusted_month:02d}_{adjusted_year}_EXPORT.xlsx"
    #previous_file_path = fr"C:\RPA\SAP\Profit_Centre\Output\{previous_year}\{previous_month:02d}\1810_{previous_month:02d}_{previous_year}_EXPORT.xlsx"

   
    previous_data = read_excel_file(previous_file_path)
    current_data = read_excel_file(current_file_path)

  
    sheets_to_process = ["A110", "A111", "A112", "A114"]

    for sheet_name in sheets_to_process:
        if sheet_name not in previous_data:
            print(f" Sheet '{sheet_name}' not found in previous file.")
            continue
        if sheet_name not in current_data:
            print(f" Sheet '{sheet_name}' not found in current file.")
            continue

        print(f"\nProcessing sheet: {sheet_name}")
        prev_sheet = previous_data[sheet_name]
        curr_sheet = current_data[sheet_name]

        updated_sheet = process_sheet(prev_sheet, curr_sheet)
        current_data[sheet_name] = updated_sheet
        print(f" Finished updating sheet: {sheet_name}")

    save_updated_excel(current_file_path, current_data)
    print("\n All updates complete.")

if __name__ == "__main__":
    main()