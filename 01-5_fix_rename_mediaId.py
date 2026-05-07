import os
import csv
import re

# ===================================================================
# 配置參數
# ===================================================================
# 請在此輸入要遍歷比對的資料夾路徑
inputpath = r'C:\Users\tiffa\Downloads\準備rename\0_shufen' 

# ===================================================================
# 核心邏輯
# ===================================================================

def fix_filenames():
    if not os.path.isdir(inputpath):
        print(f"錯誤：找不到路徑 {inputpath}")
        return

    # 1. 尋找資料夾下的參考 CSV 檔案 (參考檔案2)
    csv_files = [f for f in os.listdir(inputpath) if f.lower().endswith('.csv') and 'fixname' not in f]
    if not csv_files:
        print("錯誤：資料夾內找不到任何參考用的 .csv 檔案。")
        return
    
    csv_path = os.path.join(inputpath, csv_files[0])
    print(f"讀取參考 CSV: {csv_files[0]}")

    # 2. 預處理 CSV 中的 media_id 列表
    # 使用 set 加快完全比對速度，並保留原始列表處理模糊比對
    csv_media_ids = []
    username = ""
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                m_id = str(row.get('media_id', '')).strip()
                if m_id:
                    csv_media_ids.append(m_id)
                # 取得 username 用於輸出檔名
                if not username:
                    username = row.get('post_owner.username', 'unknown')
    except Exception as e:
        print(f"讀取 CSV 發生錯誤: {e}")
        return

    # 3. 掃描 .mp4 檔案並進行比對邏輯
    print("正在處理檔案更名與比對...")
    fix_log = []
    mp4_files = [f for f in os.listdir(inputpath) if f.lower().endswith('.mp4')]

    for original_name in mp4_files:
        # 拆解檔名：{prefix}-{timestamp}-{old_id}.mp4
        # 使用 rsplit 確保只切出最後一段 ID
        name_without_ext, ext = os.path.splitext(original_name)
        parts = name_without_ext.rsplit('-', 1)
        
        if len(parts) < 2:
            print(f"[跳過] 檔名格式不符: {original_name}")
            continue
            
        prefix_part = parts[0] # 包含 username 與 timestamp
        old_id = parts[1]
        
        target_id = None
        is_modify = 0

        # --- 三輪比對邏輯 ---
        
        # 第一輪：完全符合 media_id
        if old_id in csv_media_ids:
            target_id = old_id
            is_modify = 0
        else:
            # 第二輪：扣除右邊 1 碼比對
            for m_id in csv_media_ids:
                if len(old_id) > 1 and len(m_id) > 1:
                    if old_id[:-1] == m_id[:-1]:
                        target_id = m_id
                        is_modify = 1
                        break
            
            # 第三輪：扣除右邊 2 碼比對 (若第二輪沒對到)
            if not target_id:
                for m_id in csv_media_ids:
                    if len(old_id) > 2 and len(m_id) > 2:
                        if old_id[:-2] == m_id[:-2]:
                            target_id = m_id
                            is_modify = 1
                            break

        # 4. 執行更名與紀錄
        if target_id:
            new_name = f"{prefix_part}-{target_id}{ext}"
            
            # 實際執行更名 (若檔名真的有變動)
            if original_name != new_name:
                old_full_path = os.path.join(inputpath, original_name)
                new_full_path = os.path.join(inputpath, new_name)
                
                # 避免目的檔名已存在的衝突
                if os.path.exists(new_full_path):
                    print(f"[警告] 目的檔名已存在，跳過更名: {new_name}")
                else:
                    try:
                        os.rename(old_full_path, new_full_path)
                        print(f"[成功] {original_name} -> {new_name}")
                    except Exception as e:
                        print(f"[失敗] 更名時發生錯誤: {e}")
            
            # 無論是否異動，都記錄到 fix_log (依照需求)
            fix_log.append({
                'originName': original_name,
                'rename': new_name,
                'isModify': is_modify
            })
        else:
            # 沒對到任何 ID，也記錄下來，isModify 為 0
            fix_log.append({
                'originName': original_name,
                'rename': original_name,
                'isModify': 0
            })

    # 5. 輸出修復清單 CSV
    output_filename = f"{username}-fixname.csv"
    output_path = os.path.join(inputpath, output_filename)
    
    try:
        with open(output_path, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['originName', 'rename', 'isModify'])
            writer.writeheader()
            writer.writerows(fix_log)
        print(f"\n處理完成！修復清單已輸出至: {output_filename}")
    except Exception as e:
        print(f"輸出修復清單時發生錯誤: {e}")

if __name__ == "__main__":
    fix_filenames()