import pandas as pd
import os
from datetime import datetime

def process_csv_files(folder_path):
    # 1. 初始化一個列表來存放所有符合條件的 DataFrame
    all_filtered_data = []
    
    # 取得當前時間戳用於檔名
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"download_result_{timestamp}.csv"

    print(f"🚀 開始掃描資料夾: {folder_path}")

    # 2. 掃描資料夾內所有檔案
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            try:
                # 讀取 CSV
                df = pd.read_csv(file_path)
                
                # 檢查必要的欄位是否存在，避免程式崩潰
                required_cols = ['copy_state', 'instaloader_download_file', 'post_owner.username']
                if not all(col in df.columns for col in required_cols):
                    print(f"⚠️ 跳過 {file}: 缺少必要欄位")
                    continue

                # 3. 篩選資料行
                # 留意：'FALSE' 在 pandas 讀取時可能會被辨識為 Boolean 或 String，這裡做雙重檢查
                condition = (df['copy_state'] == 'no_file') & \
                            (df['instaloader_download_file'].astype(str).str.upper() == 'FALSE')
                
                filtered_df = df[condition]
                
                if not filtered_df.empty:
                    all_filtered_data.append(filtered_df)
                    print(f"✅ 已處理 {file}，找到 {len(filtered_df)} 筆符合資料")
                
            except Exception as e:
                print(f"❌ 讀取 {file} 時發生錯誤: {e}")

    # 4. 合併、排序並輸出
    if all_filtered_data:
        # 合併所有 DataFrame
        final_df = pd.concat(all_filtered_data, ignore_index=True)
        
        # 依照 post_owner.username (A-Z) 排序，接著依照 creation_time (早到晚) 排序
        final_df = final_df.sort_values(by=['post_owner.username', 'creation_time'], ascending=[True, True])
        
        # 輸出成 CSV
        final_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print("-" * 30)
        print(f"🎉 任務完成！")
        print(f"📂 輸出檔案：{output_filename}")
        print(f"📊 總計行數：{len(final_df)} 筆")
    else:
        print("💡 沒找到任何符合條件的資料。")

# --- 使用方式 ---
# 請將路徑替換成你存放 CSV 的資料夾路徑，例如 'C:/my_data' 或 './csv_folder'
target_folder = r'ignore\checkvedio' 
process_csv_files(target_folder)