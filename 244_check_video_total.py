import os
import pandas as pd
from tqdm import tqdm

# --- 路徑配置 ---
# 1. 妳目前在 .243 存放 CSV 的位置
index_dir = '2025_realFinal_modifyTime'
# 2. 最終存放影片的目的地 (.246 掛載在 .243 的路徑)
Video_save_dir = '/home/ftp_246/data_5/tiffany/multimodal_2026/ig_vedios/'
# 3. 妳目前放在 ig_task 裡的總表
master_list_path = 'instaloader_chcek_list.xlsx'

def main():
    if not os.path.exists(master_list_path):
        print(f"找不到原始總表: {master_list_path}")
        return

    # 讀取既有總表
    check_df = pd.read_excel(master_list_path, index_col=0)
    
    print("開始根據目的地資料夾進行最終檢核...")

    for i in tqdm(check_df.index):
        influencer_id = check_df.at[i, 'list_name_from_realFinal']
        csv_name = check_df.at[i, 'csv_name']
        
        # 目的地網紅資料夾路徑
        dest_influencer_dir = os.path.join(Video_save_dir, influencer_id)
        # 原始 CSV 路徑 (用來計算總共應該要有幾支影片)
        original_csv_path = os.path.join(index_dir, csv_name)

        if os.path.exists(original_csv_path):
            # 1. 讀取原始 CSV 算出目標總數
            try:
                temp_df = pd.read_csv(original_csv_path)
                total_expected = len(temp_df)
            except:
                total_expected = 0
            
            # 2. 計算目的地資料夾實際擁有的影片數
            if os.path.exists(dest_influencer_dir):
                # 只數 .mp4 檔案
                actual_videos = [f for f in os.listdir(dest_influencer_dir) if f.endswith('.mp4')]
                actual_count = len(actual_videos)
            else:
                actual_count = 0
            
            # 3. 更新 lost_file (目標總數 - 實際擁有的影片數)
            # 確保不會出現負數
            lost_count = max(0, total_expected - actual_count)
            check_df.at[i, 'lost_file'] = lost_count
        else:
            # 如果連原始 CSV 都找不到，標記為資訊不足 (可視情況調整)
            pass

    # 儲存為新檔案，不覆蓋舊的，比較保險
    output_path = 'instaloader_chcek_list2.xlsx'
    check_df.to_excel(output_path)
    print(f"\n檢核完成！請查看新產出的總表：{output_path}")

if __name__ == '__main__':
    main()