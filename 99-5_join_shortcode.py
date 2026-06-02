import os
import re
import shutil
import pandas as pd
from difflib import SequenceMatcher

def clean_text_advanced(text):
    """
    進階文字清洗：
    1. 移除 @帳號、#標籤
    2. 移除像 <redacted_mention> 這種系統標記
    3. 移除所有非中英文字母、數字的符號與空白，一律轉小寫
    """
    if pd.isna(text):
        return ""
    text = str(text).lower()
    
    # 移除 HTML 標記或系統變數標記 (如 <redacted_mention>)
    text = re.sub(r'<[^>]+>', '', text)
    # 移除 IG 帳號標籤 (@abc) 與 貼文標籤 (#abc)
    text = re.sub(r'@[^\s]+', '', text)
    text = re.sub(r'#[^\s]+', '', text)
    
    # 僅保留中文字、英文字母與數字，過濾所有空白、換行與 Emoji 標點
    text = re.sub(r'[^\w\u4e00-\u9fa5]', '', text)
    return text

def parse_date_to_str(date_val):
    """
    將各種格式的日期統一轉換為 'YYYY-MM-DD' 字串
    """
    if pd.isna(date_val):
        return ""
    try:
        date_str = str(date_val).strip()
        if len(date_str) >= 10:
            possible_date = date_str[:10].replace('/', '-')
            if re.match(r'^\d{4}-\d{2}-\d{2}$', possible_date):
                return possible_date
        return pd.to_datetime(date_val).strftime('%Y-%m-%d')
    except:
        return ""

def get_similarity(str1, str2):
    """計算兩字串的相似度比例 (0.0 ~ 1.0)"""
    if not str1 or not str2:
        return 0.0
    return SequenceMatcher(None, str1, str2).ratio()

def process_data_join(inputSourceDir, inputTargetDir, outputDir, sim_threshold=0.60):
    os.makedirs(outputDir, exist_ok=True)
    
    left_reel_id_list = []
    notFound_shortcode_list = []
    
    source_files = [f for f in os.listdir(inputSourceDir) if f.endswith('.xlsx') and not f.startswith('~$')]
    print(f"🚀 找到 {len(source_files)} 個來源網紅設定檔 (.xlsx)")
    
    for src_file in source_files:
        influencer_name = os.path.splitext(src_file)[0]
        target_csv_name = f"{influencer_name}-FullVideoInfo.csv"
        
        src_path = os.path.join(inputSourceDir, src_file)
        target_csv_path = os.path.join(inputTargetDir, target_csv_name)
        output_csv_path = os.path.join(outputDir, target_csv_name)
        
        if not os.path.exists(target_csv_path):
            print(f"⚠️ 警告：找不到網紅 [{influencer_name}] 的目標 CSV，跳過。")
            continue
            
        print(f"--- 正在處理網紅: {influencer_name} ---")
        shutil.copy2(target_csv_path, output_csv_path)
        
        try:
            df_src = pd.read_excel(src_path)
            df_tgt = pd.read_csv(output_csv_path, dtype={'short_code': str})
        except Exception as e:
            print(f"❌ 讀取 {influencer_name} 檔案失敗: {e}")
            continue
            
        if 'short_code' not in df_tgt.columns:
            df_tgt['short_code'] = None
        df_tgt['short_code'] = df_tgt['short_code'].astype(object)
            
        # 【前處理與 2025 年份篩選】
        df_src['clean_date'] = df_src['reel_date'].apply(parse_date_to_str)
        df_src = df_src[df_src['clean_date'] < '2026-01-01'].copy()
        
        df_src['reel_id'] = df_src['reel_id'].astype(str)
        df_src['clean_txt'] = df_src['reel_text'].apply(clean_text_advanced)
        
        df_tgt['clean_date'] = df_tgt['creation_time_tw'].apply(parse_date_to_str)
        df_tgt['clean_txt'] = df_tgt['text'].apply(clean_text_advanced)
        
        # 追蹤已分配狀態
        allocated_reel_ids = set()
        
        # ====================================================================
        # 【第一階段：全域文字完全精準匹配 (且文字在來源中唯一)】
        # ====================================================================
        txt_counts = df_src[df_src['clean_txt'] != '']['clean_txt'].value_counts()
        unique_src_texts = set(txt_counts[txt_counts == 1].index)
        unique_text_to_reel_map = df_src[df_src['clean_txt'].isin(unique_src_texts)].set_index('clean_txt')['reel_id'].to_dict()
        
        for idx, row in df_tgt.iterrows():
            tgt_text = row['clean_txt']
            if tgt_text and tgt_text in unique_text_to_reel_map:
                r_id = unique_text_to_reel_map[tgt_text]
                df_tgt.at[idx, 'short_code'] = r_id
                allocated_reel_ids.add(r_id)

        # ====================================================================
        # 【第二階段：打破日期限制的「模糊子字串包含」比對】
        # 適用場景：一邊文字被刪減、或時區跨日，但內文核心字串互相包含
        # ====================================================================
        for idx, row in df_tgt.iterrows():
            # 僅處理尚未被分配 short_code 的格子
            if pd.isna(df_tgt.at[idx, 'short_code']) or df_tgt.at[idx, 'short_code'] == '':
                tgt_text = row['clean_txt']
                if not tgt_text or len(tgt_text) < 5: # 字數太短（如：哈哈）不開子字串包含，避免誤判
                    continue
                
                # 遍歷所有尚未被配對出去的來源
                for _, src_row in df_src[~df_src['reel_id'].isin(allocated_reel_ids)].iterrows():
                    src_text = src_row['clean_txt']
                    if not src_text:
                        continue
                    
                    # 雙向檢查：不看日期，只要一段包在另一段裡面
                    if tgt_text in src_text or src_text in tgt_text:
                        df_tgt.at[idx, 'short_code'] = src_row['reel_id']
                        allocated_reel_ids.add(src_row['reel_id'])
                        break

        # ====================================================================
        # 【第三階段：結束程式前的終極強制手段 —— 全域文字相似度兜底比對】
        # 針對依然無主、且文字有所修改變形（相似度 > 60%）的物件進行配對
        # ====================================================================
        forced_count = 0
        for idx, row in df_tgt.iterrows():
            if pd.isna(df_tgt.at[idx, 'short_code']) or df_tgt.at[idx, 'short_code'] == '':
                tgt_text = row['clean_txt']
                if not tgt_text:
                    continue
                
                best_match_id = None
                max_sim = 0.0
                
                # 在剩餘未分配的設定中，找出文字相似度最高的
                for _, src_row in df_src[~df_src['reel_id'].isin(allocated_reel_ids)].iterrows():
                    src_text = src_row['clean_txt']
                    sim = get_similarity(tgt_text, src_text)
                    if sim > max_sim:
                        max_sim = sim
                        best_match_id = src_row['reel_id']
                
                # 如果相似度達到門檻，強制對齊
                if max_sim >= sim_threshold and best_match_id:
                    df_tgt.at[idx, 'short_code'] = best_match_id
                    allocated_reel_ids.add(best_match_id)
                    forced_count += 1
                    
        if forced_count > 0:
            print(f"💡 網紅 [{influencer_name}] 啟動終極模糊強制對齊，成功救回 {forced_count} 筆物件。")

        # ====================================================================
        # 儲存與日誌蒐集
        # ====================================================================
        df_tgt = df_tgt.drop(columns=['clean_date', 'clean_txt'])
        df_tgt.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        
        # 蒐集 Log 1：left_reel_id
        df_left = df_src[~df_src['reel_id'].isin(allocated_reel_ids)].copy()
        for _, row in df_left.iterrows():
            left_reel_id_list.append({
                'source': influencer_name,
                'reel_id': row['reel_id'],
                'reel_date': row['reel_date'],
                'reel_text': row['reel_text']
            })
            
        # 蒐集 Log 2：notFounx_shortcode
        df_not_found = df_tgt[df_tgt['short_code'].isna() | (df_tgt['short_code'] == '') | (df_tgt['short_code'].astype(str).str.strip() == 'nan')]
        for _, row in df_not_found.iterrows():
            notFound_shortcode_list.append({
                'post_owner.username': row.get('post_owner.username', influencer_name),
                'media_id': row.get('media_id', ''),
                'text': row.get('text', ''),
                'creation_time_tw': row.get('creation_time_tw', ''),
                'modified_time_tw': row.get('modified_time_tw', '')
            })

    # 輸出最終的兩個 Log Excel 檔案
    print("\n正在生成新一版 Log 報告...")
    
    df_log_left = pd.DataFrame(left_reel_id_list)
    if df_log_left.empty:
        df_log_left = pd.DataFrame(columns=['source', 'reel_id', 'reel_date', 'reel_text'])
    df_log_left.to_excel(os.path.join(outputDir, 'left_reel_id.xlsx'), index=False)
    
    df_log_not_found = pd.DataFrame(notFound_shortcode_list)
    if df_log_not_found.empty:
        df_log_not_found = pd.DataFrame(columns=['post_owner.username', 'media_id', 'text', 'creation_time_tw', 'modified_time_tw'])
    df_log_not_found.to_excel(os.path.join(outputDir, 'notFounx_shortcode.xlsx'), index=False)
    
    print("\n🎉 優化後的終極資料 JOIN 動作已全部完成！")

if __name__ == '__main__':
    # ==================== 請在此自訂您的資料夾路徑 ====================
    INPUT_SOURCE_DIR = r"C:\Users\tiffa\Downloads\metadata3"  # 放 66 個網紅 .xlsx 的資料夾
    INPUT_TARGET_DIR = r".\Output\Top200_VideoInfo"  # 放 一百多個 .csv 的資料夾
    OUTPUT_DIR       = r"T:\Code\Task\meta_vedio_download\Output\66_add_shortcode"       # 存放結果的資料夾
    # ================================================================
    
    process_data_join(
        inputSourceDir=INPUT_SOURCE_DIR,
        inputTargetDir=INPUT_TARGET_DIR,
        outputDir=OUTPUT_DIR,
        sim_threshold=0.60  # 文字相似度大於 60% 即可在兜底階段強制對齊（可根據執行狀況微調）
    )




    