import os
import pandas as pd
import lzma
import json
import shutil
from tqdm import tqdm
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 路徑配置 ---
index_dir = '2025_realFinal_modifyTime'
new_index_dir = '2025_realFinal_modifyTime_new'
Video_save_dir = '/home/ftp_246/data_5/tiffany/multimodal_2026/ig_vedios/' 
# 修改後的來源資料夾 (data2)
data_dir = '/home/ftp_246/data_5/多模態2026_ig_top200/data2/'
# 總表位置
master_list_path = './instaloader_chcek_list.xlsx'

def process_influencer(df, influencer_id):
    """處理單個網紅的影片搬移與 mapping"""
    source_video_dir = os.path.join(data_dir, influencer_id)
    new_video_dir = os.path.join(Video_save_dir, influencer_id)
    
    if not os.path.exists(new_video_dir):
        os.makedirs(new_video_dir)

    files = os.listdir(source_video_dir) if os.path.exists(source_video_dir) else []
    
    # 確保必要欄位存在
    for col in ['short_code', 'instaloader_download_file', 'copy_state']:
        if col not in df.columns:
            df[col] = None

    for i in range(len(df)):
        # --- 關鍵：如果已經成功過，就跳過 ---
        if df.at[i, 'copy_state'] == 'Sucess':
            continue
            
        # 取得比對用的檔名資訊
        creation_time_str = df.at[i, 'creation_time']
        dt = datetime.fromisoformat(creation_time_str)
        base_file_name = dt.strftime('%Y-%m-%d_%H-%M-%S_UTC')
        
        # 取得搬移後的命名資訊[cite: 1]
        user_name = df.at[i, 'post_owner.username']
        mod_time = df.at[i, 'modified_time_tw']
        mod_time_fmt = datetime.strptime(mod_time, "%Y-%m-%d %H:%M:%S%z").strftime("%Y%m%d%H%M%S")
        media_id = df.at[i, 'media_id']
        
        json_file = f"{base_file_name}.json.xz"
        video_file = f"{base_file_name}.mp4"

        if json_file in files:
            # 1. Short Code Mapping[cite: 1]
            try:
                with lzma.open(os.path.join(source_video_dir, json_file)) as f:
                    json_data = json.loads(f.read())
                    df.at[i, 'short_code'] = json_data.get('node', {}).get('shortcode', '')
                df.at[i, 'instaloader_download_file'] = True
            except:
                df.at[i, 'instaloader_download_file'] = False

            # 2. 影片搬移[cite: 1]
            new_file_path = os.path.join(new_video_dir, f"{user_name}-{mod_time_fmt}-{media_id}.mp4")
            try:
                if video_file in files:
                    shutil.copy(os.path.join(source_video_dir, video_file), new_file_path)
                    df.at[i, 'copy_state'] = 'Sucess' if os.path.exists(new_file_path) else 'Fail'
                else:
                    df.at[i, 'copy_state'] = 'no_video_file'
            except:
                df.at[i, 'copy_state'] = 'Fail'
        else:
            df.at[i, 'instaloader_download_file'] = False
            df.at[i, 'copy_state'] = 'no_file'

    lost_count = (df['copy_state'] != 'Sucess').sum()
    return df, lost_count

def main():
    # 讀取總表[cite: 1, 4]
    if os.path.exists(master_list_path):
        check_df = pd.read_excel(master_list_path, index_col=0)
    else:
        print("錯誤：找不到總表清單！")
        return

    # 自動偵測 data2 資料夾下的網紅名單
    target_influencers = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    print(f"偵測到需補件名單：{target_influencers}")

    for influencer in tqdm(target_influencers):
        # 尋找總表中對應的 index
        matches = check_df[check_df['list_name_from_realFinal'] == influencer]
        if matches.empty: continue
        
        idx = matches.index[0]
        csv_name = check_df.at[idx, 'csv_name']
        
        # 優先讀取「已存在的新清單」以保留舊紀錄
        new_csv_path = os.path.join(new_index_dir, csv_name)
        old_csv_path = os.path.join(index_dir, csv_name)
        
        if os.path.exists(new_csv_path):
            df = pd.read_csv(new_csv_path, index_col=0)
        else:
            df = pd.read_csv(old_csv_path)

        # 執行補件處理
        updated_df, lost_file_num = process_influencer(df, influencer)
        
        # 更新總表紀錄[cite: 1, 4]
        check_df.at[idx, 'lost_file'] = lost_file_num
        
        # 儲存個別網紅清單[cite: 2]
        updated_df.to_csv(new_csv_path)

    # 儲存更新後的總表
    check_df.to_excel(master_list_path)
    print("任務完成！")

if __name__ == '__main__':
    main()