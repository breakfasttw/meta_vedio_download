Get-ChildItem *.mp4 | Rename-Item -NewName { $_.Name.Split('-')[-1] } 
@REM 這是註解不用複製
@REM 取得最後一個dash右邊內容的檔名