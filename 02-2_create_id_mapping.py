import os
import glob
import pandas as pd

def create_ownerid_mapping(input_dir, output_filename="ownerid_mapping.csv"):

    # 1️⃣ 找到所有 Excel 檔案
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx")) + \
                  glob.glob(os.path.join(input_dir, "*.xls"))

    if not excel_files:
        print("找不到任何 Excel 檔案")
        return

    print(f"找到 {len(excel_files)} 個 Excel 檔案")

    df_list = []

    # 2️⃣ 讀取所有 Excel
    for file in excel_files:
        print("讀取:", os.path.basename(file))
        try:
            df = pd.read_excel(file)

            # 只保留需要欄位（避免讀入太多資料）
            required_cols = [
                "post_owner.id",
                "post_owner.name",
                "post_owner.username"
            ]

            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"⚠️ 欄位缺失 {missing_cols} in {file}")
                continue

            df = df[required_cols]

            df_list.append(df)

        except Exception as e:
            print(f"讀取失敗 {file}: {e}")

    if not df_list:
        print("沒有可用資料")
        return

    # 3️⃣ 合併
    combined_df = pd.concat(df_list, ignore_index=True)

    print("合併完成，共", len(combined_df), "筆")

    # 4️⃣ 去重（DISTINCT BY post_owner.id）
    combined_df = combined_df.drop_duplicates(subset=["post_owner.id"])

    print("去重後剩", len(combined_df), "筆")

    # 5️⃣ 排序（可選）
    combined_df = combined_df.sort_values(by="post_owner.id")

    # 6️⃣ 輸出 CSV
    output_path = os.path.join(input_dir, output_filename)
    combined_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("完成！輸出檔案：", output_path)


if __name__ == "__main__":
    input_folder = r"ignore\ok"
    create_ownerid_mapping(input_folder)