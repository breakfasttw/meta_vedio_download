import os
import csv
import re

def batch_rename_videos():
    # ==========================================
    # 1. 直接在這裡設定你的路徑
    # ==========================================
    # 請在引號中填入你的資料夾與 CSV 完整路徑
    VIDEO_DIR = r"C:\Users\tiffa\Downloads\二伯補檔案" 
    CSV_FILE_PATH = r"T:\Code\Task\meta_vedio_download\ignore\2025_realFinal_modifyTime\2uncle987_20260302151127.csv"
    # ==========================================

    # 檢查路徑是否存在
    if not os.path.exists(VIDEO_DIR):
        print(f"❌ 錯誤：找不到資料夾 {VIDEO_DIR}")
        return
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ 錯誤：找不到 CSV 檔案 {CSV_FILE_PATH}")
        return

    # 2. 讀取 CSV 資訊並建立索引字典
    video_info_map = {}
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 取得 creation_time 前 19 字元 (yyyy-mm-dd HH:MM:SS) 作為 Key
                c_time_key = row['creation_time'][:19]
                video_info_map[c_time_key] = {
                    'username': row['post_owner.username'],
                    'mod_time_tw': row['modified_time_tw'],
                    'media_id': row['media_id']
                }
    except Exception as e:
        print(f"❌ 讀取 CSV 失敗: {e}")
        return

    print(f"🚀 開始處理檔案...")
    success_count = 0
    fail_count = 0

    # 3. 遍歷資料夾內的檔案
    for filename in os.listdir(VIDEO_DIR):
        # 只處理 mp4 且不處理已經加過底線或改過名的檔案
        if not filename.lower().endswith(".mp4") or filename.startswith("_"):
            continue

        # 從原始檔名提取時間: 2025-12-25_10-03-09_UTC.mp4 -> 2025-12-25 10:03:09
        match = re.match(r"(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})", filename)
        
        if match:
            date_part = match.group(1)
            time_part = match.group(2).replace('-', ':')
            file_timestamp = f"{date_part} {time_part}"
            
            # 4. 比對並重新命名
            if file_timestamp in video_info_map:
                info = video_info_map[file_timestamp]
                
                # 格式化 modified_time_tw: 2025-12-28 12:43:14... -> 20251228124314
                mod_time_digits = "".join(filter(str.isdigit, info['mod_time_tw'][:19]))
                
                # 組裝新檔名
                new_name = f"{info['username']}-{mod_time_digits}-{info['media_id']}.mp4"
                
                old_path = os.path.join(VIDEO_DIR, filename)
                new_path = os.path.join(VIDEO_DIR, new_name)
                
                try:
                    os.rename(old_path, new_path)
                    print(f"✅ [成功] {filename} -> {new_name}")
                    success_count += 1
                except Exception as e:
                    print(f"❌ [失敗] 重新命名 {filename} 時出錯: {e}")
            else:
                # 未比對到資料，加底線
                old_path = os.path.join(VIDEO_DIR, filename)
                new_path = os.path.join(VIDEO_DIR, f"_{filename}")
                try:
                    os.rename(old_path, new_path)
                    print(f"⚠️ [未匹配] {filename} -> 已加底線標記")
                    fail_count += 1
                except:
                    pass
        else:
            # 檔名格式完全對不上的 (例如原本就已經是改名後的格式)
            continue

    print("-" * 30)
    print(f"✨ 處理完成！成功: {success_count} / 未匹配: {fail_count}")

if __name__ == "__main__":
    batch_rename_videos()