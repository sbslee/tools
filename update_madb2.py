import sys
import pandas as pd

def main(madb1_file, ea_file):
    df1 = pd.read_csv(madb1_file)
    df1['grch38_ea'] = '.'
    df1['status'] = 'fail'
    df1.insert(df1.columns.get_loc('grch37_alt')+1, 'grch37_ea', df1.grch38_ea.copy())
    df2 = pd.read_csv(ea_file)
    df3 = df2.merge(df1, left_on='rs_id', right_on='rs_id', how='outer')

    def one_row(r):
        r.grch37_ea = r.ea
        r.grch38_ea = r.ea

        if r.ea in [r.grch37_ref, r.grch37_alt] and r.ea in [r.grch38_ref, r.grch38_alt]:
            r.status = 'pass'
        elif r.ea != r.grch37_ref and r.ea != r.grch38_ref:
            r.grch37_alt = r.ea
            r.grch38_alt = r.ea
            r.status = 'pass'
        return r

    df3 = df3.apply(one_row, axis=1)
    df3 = df3.drop(columns='ea')
    df3.to_csv('update-madb-2.csv', index=False)

if __name__ == '__main__':
    main(*sys.argv[1:])