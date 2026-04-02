import os
import csv
from pathlib import Path

def count_mp4_in_subfolders(parent_folder, output_path):
    results = []
    
    # 確保父目錄路徑存在
    root_path = Path(parent_folder)
    if not root_path.is_dir():
        print(f"錯誤：找不到資料夾 {parent_folder}")
        return

    print(f"開始掃描資料夾: {root_path.absolute()}\n")

    # 遍歷父目錄下的所有項目
    for item in root_path.iterdir():
        # 只處理資料夾 (即每個 user 的資料夾)
        if item.is_dir():
            username = item.name
            
            # 計算該資料夾下 .mp4 的數量 (不分大小寫)
            # 如果需要搜尋子資料夾的子資料夾，可將 glob 改為 rglob
            mp4_files = [f for f in item.glob("*.mp4")]
            mp4_count = len(mp4_files)
            
            results.append({
                'username': username,
                'exist_video': mp4_count
            })
            print(f"資料夾: {username} | 找到 {mp4_count} 個 mp4")

    # 輸出最終 CSV
    save_path = Path(output_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(save_path, mode='w', encoding='utf-8-sig', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['username', 'exist_video'])
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n--- 掃描完成 ---")
        print(f"結果已儲存至: {save_path.absolute()}")
        
    except Exception as e:
        print(f"儲存檔案時發生錯誤: {e}")

if __name__ == "__main__":
    # 使用者指定輸入與輸出
    target_dir = r"/home/ftp_246/data_5/tiffany/multimodal_2026/ig_vedios/"
    result_csv = r"ignore\246_exist_count.csv"
    
    count_mp4_in_subfolders(target_dir, result_csv)