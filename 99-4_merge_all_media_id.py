import pandas as pd
import os

# --- 設定區域 ---
INPUT_DIR = r'T:\Code\Task\meta_vedio_download\Output\Top200_VideoInfo'   # 請修改為你的 CSV 存放資料夾路徑
OUTPUT_FILE = 'all_infleuncer_media_id.csv'
# ----------------

def consolidate_influencer_data():
    target_columns = [
        'post_owner.username', 
        'media_id', 
        'statistics.comment_count', 
        'statistics.like_count', 
        'duration', 
        'creation_time_tw', 
        'modified_time_tw', 
        'is_file_exist'
    ]
    
    data_frames = []
    
    # 遍歷資料夾中的所有檔案
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.csv'):
            file_path = os.path.join(INPUT_DIR, filename)
            
            try:
                # 讀取 CSV
                df = pd.read_csv(file_path)
                
                # 檢查目標欄位是否存在
                missing_cols = [col for col in target_columns if col not in df.columns]
                
                if not missing_cols:
                    # 若欄位齊全，保留目標欄位並加入列表
                    data_frames.append(df[target_columns])
                    print(f"成功處理: {filename}")
                else:
                    print(f"跳過檔案 {filename}: 缺少欄位 {missing_cols}")
                    
            except Exception as e:
                print(f"無法讀取檔案 {filename}: {e}")

    # 合併所有資料
    if data_frames:
        final_df = pd.concat(data_frames, ignore_index=True)
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n全部處理完畢！共合併 {len(data_frames)} 個檔案，輸出至: {OUTPUT_FILE}")
    else:
        print("未找到任何符合欄位條件的 CSV 檔案。")

if __name__ == "__main__":
    consolidate_influencer_data()