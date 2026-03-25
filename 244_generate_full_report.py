import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# --- 路徑配置 ---
index_dir = '2025_realFinal_modifyTime'
Video_save_dir = '/home/ftp_246/data_5/tiffany/multimodal_2026/ig_vedios/'
output_file = 'all_videos_final_report.csv'

def main():
    if not os.path.exists(index_dir):
        print(f"錯誤：找不到來源資料夾 {index_dir}")
        return

    csv_files = [f for f in os.listdir(index_dir) if f.endswith('.csv')]
    all_data_frames = []

    print(f"開始掃描 {len(csv_files)} 位網紅的影片狀態...")

    for csv_name in tqdm(csv_files):
        csv_path = os.path.join(index_dir, csv_name)
        try:
            df = pd.read_csv(csv_path)
            
            # --- 關鍵修正：檢查欄位是否存在 ---
            required_cols = ['post_owner.username', 'media_id', 'modified_time_tw']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                # 如果缺欄位，就跳過這個人，並在 Terminal 提示
                # print(f"\n[跳過] 檔案 {csv_name} 缺少欄位: {missing_cols}")
                continue

            results = []
            for _, row in df.iterrows():
                user_name = str(row['post_owner.username'])
                media_id = str(row['media_id'])
                mod_time = str(row['modified_time_tw'])
                
                try:
                    # 轉換時間格式
                    mod_time_fmt = datetime.strptime(mod_time, "%Y-%m-%d %H:%M:%S%z").strftime("%Y%m%d%H%M%S")
                    expected_filename = f"{user_name}-{mod_time_fmt}-{media_id}.mp4"
                except:
                    expected_filename = "TIME_FORMAT_ERROR"

                # 檢查檔案
                dest_path = os.path.join(Video_save_dir, user_name, expected_filename)
                file_exists = os.path.exists(dest_path)
                
                results.append({
                    'influencer': user_name,
                    'media_id': media_id,
                    'creation_time': row.get('creation_time', ''),
                    'expected_filename': expected_filename,
                    'is_file_exist': 'Yes' if file_exists else 'No',
                    'csv_source': csv_name
                })
            
            if results:
                all_data_frames.append(pd.DataFrame(results))
                
        except Exception as e:
            # 避免任何因單一 CSV 損壞導致的程式中斷
            continue

    # 合併結果
    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n[成功] 總表已產出：{output_file}")
        print(f"總計影片數: {len(final_df)}")
        print(f"已完成數: {len(final_df[final_df['is_file_exist'] == 'Yes'])}")
        print(f"缺失數: {len(final_df[final_df['is_file_exist'] == 'No'])}")
    else:
        print("\n[錯誤] 沒有抓取到任何有效的資料。")

if __name__ == '__main__':
    main()