import pandas as pd
import glob
import os
from datetime import datetime
import pytz

# ===============================
# 基本設定
# ===============================
INPUT_DIR = r"ignore/ok"   # 放 Excel 的資料夾
OUTPUT_DIR = r"ignore/ouput/todo_list_person"

SELECTED_COLUMNS = [
    'creation_time',
    'post_owner.id',
    'post_owner.name',
    'post_owner.username',
    'id',
    'media_id',
    'tags'

]

# 指定帳號清單
TARGET_NAMES = {
        "yga0721","joemanweng","fumeancat","khshu_","elephant_gogo",
        "1006nk","howhowhasfriends","pansci","nanaciaociao","arielsvlog_816",
        "basil_77777","minikiki_0529","rayduenglish","ladyflavor",
        "ladyflavor_cui","ladyflavor_nowpo","jaychang0127","teacherchiang",
        "lessonsfrommovies","soybeanmilk_cat","onionman__","baxuan_ig",
        "jam_cutepig","andy1994x","imseriou","sanyuan_japan","bugcat_capoo",
        "thedodomen","huangbrotherss","weiteng0710","linda831212","87acup",
        "k3okii","energydessert2019","thetiffanychen","annie72127",
        "shasha77.daily","helloiamhook","muyao4","dannybeeech",
        "lynnwu0219_2","hahatai_official","dinteroffical","peter_and_susan",
        "hanhanpovideo","allie.liao","nickwang1988","tsaigray2018",
        "ruruspiano","achusan0817","achu08177","da_chien_huang",
        "goodalicia","beauty___wu","coindevanity","peeta.gege",
        "milasky_love","corgimango","xollox121","aries_8248",
        "rosalina.kitchen","uccu0323","thisgroupofpeople","3ctim",
        "peri168","twodeerman","ethan_kuan_kuan","gu_yuze","ginachiki",
        "daddy.iam","egg_things","yu.young_fate","zoebitalk","cb.otaku",
        "loserzun","chuchumei__","flashwolves2013","benwoooo",
        "sundayright_dub","riceandshine.co","maomaotv","liketaitai",
        "ala780705","colorfulinsideus","602avenue","crown_du",
        "hailey_jane","5inana","gary.0917","jasondchen","taiwanniu421",
        "cheapaoe","wanghungche","miao11255","children0704",
        "lobo_is_good","lovemiihuang","bimay_nagging","sandykaka_",
        "davio_magic","cocowine0205","martinispig","aottergirls",
        "jengsu","shirley_01_12","ironbullim","tiest0913",
        "tsaibrotherderboofan","ciao_kitchen","tesscube",
        "sandymandy_official","mochi_dad","6tan","lilyflute",
        "wecook123123","lingin1209","niniru621","alvin701",
        "de.jun_666","yuniko0720","超級熱狗王 林敬傑 IFBB PRO",
        "bobo_kira","2uncle987","traveggo","alephant_0427","eatzzz7",
        "taiwan_bar","pattihuang","kaoru_0409","daisy.chiu",
        "hithisisachi","jbao_5000","donehannah","inesinesw","1105ya",
        "stanley_sportslife","cyfang58","swhite523","tzu_888",
        "dr.acne_scar_queen","mensgametw","20141010hero","bonibaobao",
        "juliamisakii","kart_wang","shaogao","lan0716","peter825",
        "manatee0701","coffee_time_corgi","ruwenwen","xiucao.han",
        "lamashania","three_muggles","uni_catto","emmy_on_earth",
        "logandbeck","1_shiuan_0","japanuts","campfire_tw",
        "blairechen","cliffchuang3","mengj215","itsberrym","afunnywii",
        "mayukichou","huzihuang1989","nana_aleng","tinana_master",
        "weisway18","neneko.n","wia627","jam_steak","getwie__",
        "iam_3636","daiying0","wufeili","zxsdexz","kellyshen40",
        "oldwangstock","mypink0911","hooleefun","walkerdad1228",
        "boyplaymj","b2btramy888888","cindyhhh32","mr.joehobby",
        "sunnie_cat0111","goris.sky","lebaby0v0","byleway",
        "nowyouon","0_shufen","momanddad_band","maygobla",
        "kai_makeup_","skauramomo","ninggoose","anjouclever",
        "linzin.yt","thechef_fred","hsin0126"
    }

# ===============================
# 主流程
# ===============================
def process_pipeline():

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    excel_files = glob.glob(os.path.join(INPUT_DIR, "*.xlsx")) + \
                  glob.glob(os.path.join(INPUT_DIR, "*.xls"))

    if not excel_files:
        print("找不到任何 Excel 檔案")
        return

    print(f"找到 {len(excel_files)} 個 Excel 檔案")

    df_list = []

    taiwan_tz = pytz.timezone("Asia/Taipei")
    start_time = taiwan_tz.localize(datetime(2025,1,1,0,0,0))
    end_time   = taiwan_tz.localize(datetime(2025,12,31,23,59,59))

    for file in excel_files:
        print("讀取:", os.path.basename(file))
        df = pd.read_excel(file)

        # 保留必要欄位
        existing_cols = [col for col in SELECTED_COLUMNS if col in df.columns]
        df = df[existing_cols].copy()

        # 篩選帳號
        df = df[df['post_owner.username'].isin(TARGET_NAMES)]
        if df.empty:
            continue

        # 轉換時間（假設為 ISO 或 UTC）
        df['creation_time'] = pd.to_datetime(df['creation_time'], utc=True)
        df['creation_time_tw'] = df['creation_time'].dt.tz_convert(taiwan_tz)

        # 篩選 2025 年
        df = df[
            (df['creation_time_tw'] >= start_time) &
            (df['creation_time_tw'] <= end_time)
        ]

        # ===============================
        # 🔥 新增：只保留 tags 有值的資料
        # ===============================
        df = df[
            df['tags'].notna() &
            (df['tags'].astype(str).str.strip() != "")
        ]


        # 轉換最終dataframe
        if not df.empty:
            df_list.append(df)

    if not df_list:
        print("沒有符合條件的資料")
        return

    # 合併
    combined_df = pd.concat(df_list, ignore_index=True)

    print("合併完成，共", len(combined_df), "筆")

    # ===============================
    # 🔥 新需求：依 media_id 去重
    # ===============================
    combined_df = combined_df.drop_duplicates(subset=["media_id"])

    print("media_id 去重後剩", len(combined_df), "筆")

    combined_df = combined_df.sort_values(
        by=['post_owner.username', 'creation_time_tw']
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # 分帳號輸出
    for user, user_df in combined_df.groupby('post_owner.username'):
        safe_user = str(user).replace('/', '_').replace('\\', '_')
        output_filename = f"{safe_user}_{timestamp}.csv"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        user_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print("已輸出:", output_filename)

    print("全部完成")

# ===============================
if __name__ == "__main__":
    process_pipeline()