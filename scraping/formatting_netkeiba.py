import pandas as pd

# CSVファイルを読み込む
df = pd.read_csv('results/race_result_consolidated.csv')

# データの確認
print("変換前のデータ例：")
print("日付例：", df['race_date'].head())
print("時刻例：", df['start_time'].head())
print("着順例：", df['着順'].head())
print("単勝例：", df['単勝'].head())

# 日付形式を変換（順序を修正）
df['race_date'] = (df['race_date']
                  .str.replace('年', '-')
                  .str.replace('月', '-')
                  .str.replace('日', ''))
df['race_date'] = pd.to_datetime(df['race_date'])
df['race_date'] = df['race_date'].dt.strftime('%Y-%m-%d')

# 時刻形式を変換（HH:MM → HH:MM:00）
df['start_time'] = df['start_time'] + ':00'

# 着順カラムの特殊な値を処理
# 数値以外の値（中止、除外、取消など）を-1に変換
df['着順'] = pd.to_numeric(df['着順'], errors='coerce').fillna(-1).astype(int)

# 単勝オッズの処理
# "---"などの特殊な値を-1に変換
df['単勝'] = df['単勝'].replace('---', '-1')  # 特殊な値を-1に置換
df['単勝'] = pd.to_numeric(df['単勝'], errors='coerce').fillna(-1)  # 数値変換できない値も-1に

# 変換後のデータを確認
print("\n変換後のデータ例：")
print("日付例：", df['race_date'].head())
print("時刻例：", df['start_time'].head())
print("着順例：", df['着順'].head())
print("単勝例：", df['単勝'].head())

# 変換したデータを保存
df.to_csv('results/race_result_formatted.csv', index=False)
