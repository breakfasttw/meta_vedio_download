# 將檔名所對應的{網紅名稱}_影片清單.csv放到和影片相同的資料夾內
# 輸入目標資料夾路徑 (本機內的都可以)
# 命名規則 {網紅英數username}-{格式化後的modify_time_tw}-{media_id}.mp4
# 例如：cocowine0205-20250105115500-790138397471530.mp4

import os
import csv
import re
from datetime import datetime
from collections import Counter

# ======================== 輸入影片檔案資料夾路徑=====================
input_dir = r"C:\Users\tiffa\Downloads\tsaibrotherderboofan"
# ===================================================================

def format_timestamp(ts_str):
    """將 2025-01-09 17:27:06+08:00 轉換為 20250109172706"""
    # 提取前 19 個字元 (YYYY-MM-DD HH:MM:SS) 並移除所有非數字
    clean_ts = re.sub(r'\D', '', ts_str[:19])
    return clean_ts

def batch_rename_logic():
    print("開始執行檔案比對與轉換")
    # 讓使用者輸入資料夾路徑
    
    if not os.path.isdir(input_dir):
        print(f"錯誤：找不到路徑 {input_dir}")
        return

    # 1. 尋找資料夾下的 CSV 檔案
    csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.csv')]
    if not csv_files:
        print("錯誤：資料夾內找不到任何 .csv 檔案。")
        return
    
    # 假設使用資料夾內找到的第一個 CSV
    csv_path = os.path.join(input_dir, csv_files[0])
    print(f"讀取查找表: {csv_files[0]}")

    # 2. 預處理 CSV 資料
    csv_data = []
    try:
        print("正在進行csv欲處理....")
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                username = row.get('post_owner.username', 'unknown')
                raw_time = row.get('modified_time_tw', '')
                m_id = row.get('media_id', '')
                p_id = row.get('id', '')
                
                formatted_time = format_timestamp(raw_time)
                # 建立目標新檔名
                target_new_name = f"{username}-{formatted_time}-{m_id}.mp4"
                
                csv_data.append({
                    'target_new_name': target_new_name,
                    'media_id': str(m_id),
                    'id': str(p_id),
                    'is_used': False # 標記此 CSV 列是否已被分配
                })
    except Exception as e:
        print(f"讀取 CSV 發生錯誤: {e}")
        return

    # 3. 掃描資料夾內的 .mp4 檔案
    print("正在掃描資料夾內的檔案...")
    mp4_files = []
    for f in os.listdir(input_dir):
        if f.lower().endswith('.mp4'):
            mp4_files.append({
                'original_name': f,
                'pure_id': os.path.splitext(f)[0],
                'target_name': None,
                'match_type': None
            })

    # 4. 四輪比對邏輯 (確保精確優先於模糊)
    # 第一輪：完全符合 media_id
    print("正在執行【第一輪】檔案邏輯比對...")
    for file_obj in mp4_files:
        for row in csv_data:
            if not row['is_used'] and file_obj['pure_id'] == row['media_id']:
                file_obj['target_name'] = row['target_new_name']
                file_obj['match_type'] = "Exact (media_id)"
                row['is_used'] = True
                break

    # 第二輪：完全符合 id (排除已匹配)
    print("正在執行【第二輪】檔案邏輯比對...")
    for file_obj in mp4_files:
        if file_obj['target_name']: continue
        for row in csv_data:
            if not row['is_used'] and file_obj['pure_id'] == row['id']:
                file_obj['target_name'] = row['target_new_name']
                file_obj['match_type'] = "Exact (id)"
                row['is_used'] = True
                break

    # 第三輪：扣除末 1 碼符合 media_id
    print("正在執行【第三輪】檔案邏輯比對...")
    for file_obj in mp4_files:
        if file_obj['target_name']: continue
        for row in csv_data:
            if not row['is_used'] and len(file_obj['pure_id']) > 1 and len(row['media_id']) > 1:
                if file_obj['pure_id'][:-1] == row['media_id'][:-1]:
                    file_obj['target_name'] = row['target_new_name']
                    file_obj['match_type'] = "Fuzzy (minus 1 char)"
                    row['is_used'] = True
                    break

    # 第四輪：扣除末 2 碼符合 media_id
    print("正在執行【第四輪】檔案邏輯比對...")
    for file_obj in mp4_files:
        if file_obj['target_name']: continue
        for row in csv_data:
            if not row['is_used'] and len(file_obj['pure_id']) > 2 and len(row['media_id']) > 2:
                if file_obj['pure_id'][:-2] == row['media_id'][:-2]:
                    file_obj['target_name'] = row['target_new_name']
                    file_obj['match_type'] = "Fuzzy (minus 2 chars)"
                    row['is_used'] = True
                    break

    # 5. 衝突檢查與實際執行
    # 統計每個目標檔名被分配到的次數
    target_counts = Counter([f['target_name'] for f in mp4_files if f['target_name']])
    
    print("\n--- 準備執行更名作業 ---")
    
    for file_obj in mp4_files:
        old_name = file_obj['original_name']
        new_name = file_obj['target_name']
        
        # 情況 A: 沒找到匹配項
        if not new_name:
            print(f"[跳過] {old_name} (找不到對應 ID)")
            continue
            
        # 情況 B: 偵測到檔名衝突 (多個檔案對應到同一個新檔名)
        if target_counts[new_name] > 1:
            print(f"[錯誤] {old_name} -> 發生檔名衝突 ({new_name})，取消此兩筆更名動作。")
            continue

        # 情況 C: 執行更名
        old_path = os.path.join(input_dir, old_name)
        new_path = os.path.join(input_dir, new_name)
        
        # 檢查新檔名是否已存在於磁碟 (防止覆蓋現有檔案)
        if os.path.exists(new_path) and old_name != new_name:
            print(f"[跳過] {old_name} -> 目的檔案 {new_name} 已存在。")
            continue

        try:
            os.rename(old_path, new_path)
            print(f"[成功] {old_name} -> {new_name} ({file_obj['match_type']})")
        except Exception as e:
            print(f"[失敗] {old_name} 執行時發生錯誤: {e}")

    print("\n所有處理已結束。")

if __name__ == "__main__":
    batch_rename_logic()