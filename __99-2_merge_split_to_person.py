import pandas as pd
import glob
import os
from datetime import datetime
import pytz

def merge_and_process_csv(input_dir, output_dir):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_files = glob.glob(os.path.join(input_dir, "*.csv"))
    print(f"找到 {len(all_files)} 個檔案")

    # ===============================
    # ✅ 指定帳號清單
    # ===============================
    target_names = {
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

    df_list = []

    taiwan_tz = pytz.timezone("Asia/Taipei")
    start_time = taiwan_tz.localize(datetime(2025,1,1,0,0,0))
    end_time   = taiwan_tz.localize(datetime(2025,12,31,23,59,59))

    for filename in all_files:

        temp_df = pd.read_csv(filename)

        # ✅ 篩選指定帳號
        temp_df = temp_df[temp_df['post_owner.username'].isin(target_names)]

        if temp_df.empty:
            continue

        # ✅ 時間轉換（假設 creation_time 為 Unix timestamp）
        temp_df['creation_time'] = pd.to_datetime(
            temp_df['creation_time'],
            unit='s',
            utc=True
        )

        temp_df['creation_time_tw'] = temp_df['creation_time'].dt.tz_convert(taiwan_tz)

        # ✅ 篩選 2025 年（台灣時間）
        temp_df = temp_df[
            (temp_df['creation_time_tw'] >= start_time) &
            (temp_df['creation_time_tw'] <= end_time)
        ]

        if not temp_df.empty:
            df_list.append(temp_df)

    if not df_list:
        print("沒有符合條件的資料")
        return

    combined_df = pd.concat(df_list, ignore_index=True)

    combined_df = combined_df.sort_values(
        by=['post_owner.username', 'creation_time_tw']
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    for user, user_df in combined_df.groupby('post_owner.username'):
        safe_user = str(user).replace('/', '_').replace('\\', '_')
        output_filename = f"{safe_user}_{timestamp}.csv"
        output_path = os.path.join(output_dir, output_filename)
        user_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print("完成")


if __name__ == "__main__":
    # 在這裡設定你的輸入與輸出路徑
    if __name__ == "__main__":
        input_folder = r"ignore\ouput\todo_list"
        output_folder = r"ignore\ouput\todo_list_person"

        merge_and_process_csv(input_folder, output_folder)