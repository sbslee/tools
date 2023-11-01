import pandas as pd

df1 = pd.read_csv('temp.csv')
df1['grch37_ea'] = '.'
df1['grch38_ea'] = '.'
df2 = pd.read_csv('ea.csv')
df3 = df2.merge(df1, left_on='rs_id', right_on='rs_id')

def one_row(r):
    r.grch37_ea = r.ea
    r.grch38_ea = r.ea
    if r.ea not in [r.grch37_ref, r.grch37_alt]:
        r.grch37_alt = r.ea + '@'
    if r.ea not in [r.grch38_ref, r.grch38_alt]:
        r.grch38_alt = r.ea + '@'
    return r

df3 = df3.apply(one_row, axis=1)
df3.to_csv('test.csv', index=False)