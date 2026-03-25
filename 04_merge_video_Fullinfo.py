import pandas as pd
import os
import glob

# ==========================================
# 參數編輯區域
# ==========================================
DIR_A = r'T:\Code\Task\meta_vedio_download\ignore\ouput\todo_list_person\2025_200fullinfo'          # 資料夾 A 路徑
DIR_B = r'C:\Users\tiffa\Downloads\2025_realFinal_modifyTime_new\2025_realFinal_modifyTime_new2'          # 資料夾 B 路徑
FILE_C = r'C:\Users\tiffa\Downloads\2025_realFinal_modifyTime_new\all_videos_final_report.csv' # 檔案 C 的路徑
OUTPUT_DIR = r'Output\VideoInfo'       # 輸出資料夾路徑
# ==========================================

def process_csv_joining():
    # 確保輸出目錄存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"建立輸出目錄: {OUTPUT_DIR}")

    # 1. 預先讀取檔案 C (總表)
    print("讀取檔案 C...")
    if not os.path.exists(FILE_C):
        print(f"錯誤：找不到檔案 C ({FILE_C})")
        return
        
    df_c = pd.read_csv(FILE_C)
    # 確保關鍵欄位格式一致 (轉為字串避免型別錯誤)
    df_c['media_id'] = df_c['media_id'].astype(str)
    # 只取需要的欄位
    df_c = df_c[['media_id', 'influencer', 'is_file_exist']]

    # 2. 獲取資料夾 A 內的所有 CSV
    files_a = glob.glob(os.path.join(DIR_A, "*.csv"))
    
    if not files_a:
        print("資料夾 A 內找不到任何 CSV 檔案。")
        return

    for file_path_a in files_a:
        # --- 修正處：從檔名提取 username ---
        # 使用 rsplit('_', 1) 從右邊開始切分，只切一次
        # 範例：'1_shiuan_0_20260320114521.csv' -> ['1_shiuan_0', '20260320114521.csv']
        filename_a = os.path.basename(file_path_a)
        username = filename_a.rsplit('_', 1)[0]
        
        print(f"正在處理網紅: {username} ...")

        # 讀取資料夾 A 的檔案
        df_a = pd.read_csv(file_path_a)
        df_a['media_id'] = df_a['media_id'].astype(str)

        # 3. 嘗試與資料夾 B 整合
        # 尋找資料夾 B 中屬於該 username 的檔案 (同樣使用格式比對)
        files_b = glob.glob(os.path.join(DIR_B, f"{username}_*.csv"))
        
        if files_b:
            # 讀取 B 資料夾中對應的檔案
            df_b = pd.read_csv(files_b[0])
            df_b['media_id'] = df_b['media_id'].astype(str)
            # 只取 media_id 和 short_code，並去重
            df_b_subset = df_b[['media_id', 'short_code']].drop_duplicates(subset=['media_id'])
            df_final = pd.merge(df_a, df_b_subset, on='media_id', how='left')
        else:
            df_final = df_a.copy()
            df_final['short_code'] = None

        # 4. 與檔案 C 整合 (同時核對 media_id 與 influencer)
        df_c_influencer = df_c[df_c['influencer'] == username]
        df_final = pd.merge(df_final, df_c_influencer[['media_id', 'is_file_exist']], on='media_id', how='left')

        # 5. 判斷 DownloadTool 邏輯
        def determine_tool(row):
            # 統一轉為字串處理，避免 NaN 造成問題
            is_exist = str(row.get('is_file_exist', '')).strip()
            short_code = row.get('short_code')
            
            # 判斷 short_code 是否為空
            is_short_code_empty = pd.isna(short_code) or str(short_code).strip() == "" or str(short_code).lower() == 'nan'

            if is_exist == 'Yes':
                if not is_short_code_empty:
                    return "Instaloader"
                else:
                    return "MetaTool"
            elif is_exist == 'No':
                return ""
            else:
                return ""

        df_final['DownloadTool'] = df_final.apply(determine_tool, axis=1)

        # 6. 輸出檔案
        output_filename = f"{username}-FullVideoInfo.csv"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"完成輸出: {output_filename}")

if __name__ == "__main__":
    process_csv_joining()
    print("\n所有程序已執行完畢。")