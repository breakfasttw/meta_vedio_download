import os
import csv
from pathlib import Path

def process_csv_files(input_folder, output_path):
    results = []
    
    # 確保輸入路徑存在
    folder_path = Path(input_folder)
    if not folder_path.is_dir():
        print(f"錯誤：找不到路徑 {input_folder}")
        return

    # 掃描資料夾下所有的 .csv 檔案
    for file_path in folder_path.glob("*.csv"):
        # 排除輸出的目標檔案，避免重複讀取
        if file_path.name == Path(output_path).name:
            continue
            
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                # 計算總行數
                row_count = sum(1 for line in f)
                
                # 計算所需的數量 (總行數 - 1 表頭)
                # 若檔案為空，則設為 0
                required_count = max(0, row_count - 1)
                
                # 處理檔名邏輯：扣除右邊 19 位數
                # 範例：username_20231027123000.csv -> username
                filename = file_path.name
                username = filename[:-19]
                
                results.append({
                    'username': username,
                    'required_count': required_count
                })
                print(f"已處理: {filename}")
                
        except Exception as e:
            print(f"處理檔案 {file_path.name} 時發生錯誤: {e}")

    # 輸出最終 CSV
    save_path = Path(output_path)
    # 確保輸出目錄存在
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(save_path, mode='w', encoding='utf-8', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=['username', 'required_count'])
        writer.writeheader()
        writer.writerows(results)

    print(f"\n--- 統計完成 ---")
    print(f"結果已儲存至: {save_path.absolute()}")

if __name__ == "__main__":
    # --- 你可以在這裡設定路徑 ---
    src_dir = r"T:\Code\Task\meta_vedio_download\ignore\2025_realFinal_modifyTime"
    out_file = r"ignore\required_count.csv"
    
    process_csv_files(src_dir, out_file)