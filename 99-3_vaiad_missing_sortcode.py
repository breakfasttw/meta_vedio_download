import os
import pandas as pd

def filter_missing_short_code(input_dir, output_file):
    # 確保輸入目錄存在
    if not os.path.exists(input_dir):
        print(f"錯誤：找不到目錄 {input_dir}")
        return

    # 取得資料夾內所有的 csv 檔案路徑
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print("資料夾內沒有 csv 檔案。")
        return

    combined_data = []
    header_saved = False
    target_column = 'short_code'

    for i, file_name in enumerate(csv_files):
        file_path = os.path.join(input_dir, file_name)
        
        try:
            # 讀取 CSV
            df = pd.read_csv(file_path)

            # 檢查是否存在目標欄位
            if target_column not in df.columns:
                print(f"跳過檔案 {file_name}：未找到欄位 [{target_column}]")
                continue

            # 找出 short_code 為空值（NaN）或空字串的資料
            # 使用 .isna() 捕捉 NaN，並過濾掉轉換成字串後為空的資料
            missing_df = df[df[target_column].isna() | (df[target_column].astype(str).str.strip() == '')]

            if not missing_df.empty:
                combined_data.append(missing_df)
                print(f"從 {file_name} 提取了 {len(missing_df)} 行資料")

        except Exception as e:
            print(f"處理檔案 {file_name} 時發生錯誤: {e}")

    # 如果有蒐集到資料，則進行合併並匯出
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        # 依照需求，這裏會自動保留第一個 DataFrame 的欄位順序（表頭）
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n處理完成！結果已儲存至：{output_file}")
        print(f"總計提取行數：{len(final_df)}")
    else:
        print("\n未發現符合條件（short_code 為空）的資料。")

if __name__ == "__main__":
    # 設定輸入資料夾與輸出檔名
    INPUT_DIRECTORY = 'T:\Code\Task\meta_vedio_download\Output\Top200_VideoInfo'
    OUTPUT_FILENAME = 'shortcode_valiad.csv'

    filter_missing_short_code(INPUT_DIRECTORY, OUTPUT_FILENAME)