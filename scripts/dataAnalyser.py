import pandas as pd

df=pd.read_parquet(r"C:\Users\Ishika\words-drift\data\processed\economics\RC_2010-01_subreddit_count.parquet")

print(df.head())