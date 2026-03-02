import os
import glob
import csv

def count_csv_rows(input_dir, split_token="_20260302115844"):

    all_files = glob.glob(os.path.join(input_dir, "*.csv"))

    results = []

    for filepath in all_files:

        filename = os.path.basename(filepath)

        # 只處理含有指定字串的檔案
        if split_token not in filename:
            continue

        # 擷取網紅名稱
        name = filename.split(split_token)[0]

        # 計算行數（扣除表頭）
        with open(filepath, 'r', encoding='utf-8') as f:
            row_count = sum(1 for _ in f) - 1

        if row_count < 0:
            row_count = 0

        results.append((name, row_count))

    # 輸出總清單
    output_path = os.path.join(input_dir, "total_vedio_task.csv")

    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["name", "amount"])
        writer.writerows(results)

    print("完成！輸出檔案：", output_path)


if __name__ == "__main__":
    input_folder = r"ignore\ouput\todo_list_person"
    count_csv_rows(input_folder)