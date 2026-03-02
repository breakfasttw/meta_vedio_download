import pandas as pd
from datetime import datetime
import pytz
import os

# ================= 配置區域 =================
# 1. 自定義目錄與檔名
INPUT_DIR = 'ignore'
INPUT_FILENAME = 'asiaKol_186-199_N=13_videoOnly.xlsx'  # 請修改為你的檔名
OUTPUT_DIR = r"ignore/ouput/todo_list"
filename_without_ext = os.path.splitext(INPUT_FILENAME)[0]
OUTPUT_FILENAME = f"{filename_without_ext}.csv"

# 2. 自定義所需欄位 (可自行增減)
SELECTED_COLUMNS = [
    'creation_time', 
    'post_owner.name', 
    'post_owner.username', 
    'url', 
    'id', 
    'media_id'
]
# ===========================================

def convert_to_tw_time(time_str):
    """
    將 ISO 格式時間字串轉換為台灣時區 (UTC+8) 並格式化為 yyyymmddHHMMSS
    """
    try:
        # 轉換為 pandas datetime 物件 (自動處理 +00:00 等時區偏移)
        dt = pd.to_datetime(time_str)
        
        # 設定目標時區為台灣
        tw_tz = pytz.timezone('Asia/Taipei')
        
        # 如果原始資料沒有時區資訊，假設它是 UTC；如果有，則直接轉時區
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        
        dt_tw = dt.astimezone(tw_tz)
        
        # 格式化輸出
        return dt_tw.strftime('%Y%m%d%H%M%S')
    except Exception as e:
        print(f"時間轉換錯誤: {time_str} -> {e}")
        return None

def process_file():
    input_path = os.path.join(INPUT_DIR, INPUT_FILENAME)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

    print(f"正在讀取檔案: {input_path}...")
    
    # 根據副檔名選擇讀取方式
    if input_path.endswith('.csv'):
        df = pd.read_csv(input_path)
    else:
        # 預設為 excel
        df = pd.read_excel(input_path)

    # 檢查欄位是否存在，並提取
    existing_cols = [col for col in SELECTED_COLUMNS if col in df.columns]
    missing_cols = set(SELECTED_COLUMNS) - set(existing_cols)
    if missing_cols:
        print(f"警告：找不到以下欄位 {missing_cols}")
    
    df_filtered = df[existing_cols].copy()

    # 3. 轉換時間格式
    if 'creation_time' in df_filtered.columns:
        print("正在轉換時間格式至台灣時區...")
        df_filtered['creation_time_formatted'] = df_filtered['creation_time'].apply(convert_to_tw_time)
    
    # 儲存為 UTF-8 CSV (使用 utf-8-sig 可確保 Excel 開啟不亂碼)
    
    df_filtered.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"處理完成！檔案已儲存至: {output_path}")
    print(df_filtered[['creation_time', 'creation_time_formatted']].head()) # 顯示前幾行確認

if __name__ == "__main__":
    process_file()