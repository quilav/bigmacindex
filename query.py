import pandas as pd
from pandas import DataFrame

mdf = pd.read_csv('big-mac-index.csv')

semi_final = mdf.sort_values(['dollar_ppp', 'code'], ascending=False).groupby('code').head(1)
final = semi_final.nlargest(5, 'dollar_ppp')

print(final)
final.to_csv('top5-bmi.csv')