import pandas as pd

# CSVファイルを読み込む
df = pd.read_csv('results/race_result_consolidated.csv')

# データの確認
print("変換前の日付の例：")
print(df['race_date'].head())

# 日付形式を変換
df['race_date'] = (df['race_date']
                   .str.replace('年', '-')
                   .str.replace('月', '-')
                   .str.replace('日', ''))
df['race_date'] = pd.to_datetime(df['race_date'])
df['race_date'] = df['race_date'].dt.strftime('%Y-%m-%d')

# 変換後のデータを確認
print("\n変換後の日付の例：")
print(df['race_date'].head())

# 変換したデータを保存
df.to_csv('results/race_result_formatted.csv', index=False)
