import sys
import pandas as pd

def check_status(df1 , df2):
    def one_row(r):
        df = df1[df1.rs_id == r.rs_id]
        if df.empty:
            r.status = 'absent'
        elif df.shape[0] == 1:
            if df.grch37_ea.values[0] == r.grch37_ea and df.grch38_ea.values[0] == r.grch38_ea:
                r.status = 'present'
            elif df.grch37_ea.values[0] == '.' and df.grch38_ea.values[0] == '.':
                tmp = df2[df2.rs_id == r.rs_id]
                if tmp.shape[0] == 1:
                    r.status = 'fill'
                else:
                    r.status = 'progress'
            else:
                r.status = 'absent'
        else:
            if r.grch37_ea in df.grch37_ea.values and r.grch38_ea in df.grch38_ea.values:
                r.status = 'present'
            else:
                r.status = 'progress'
        return r
    df3 = df2.apply(one_row, axis=1)
    df3.to_csv('test.csv')
#     if not df3[df3.status == 'progress'].empty:
#         raise ValueError()
    return df3

def append_absent_data(df1, df2):
    df3 = df2[df2.status == 'absent']
    df3 = df3.drop(columns='status')
    df3['set_jpn_disease'] = 'yes'
    df4 = pd.concat([df1, df3])
    df4 = df4.fillna('.')
    return df4
    
def fill_missing_ea(df1, df2):
    df3 = df2[df2.status == 'fill']
    def one_row(r):
        if r.grch37_ea != '.' and r.grch38_ea != '.':
            return r
        df = df3[(df3.rs_id == r.rs_id)]
        if df.empty:
            return r
        if df.shape[0] == 1:
            r.grch37_ea = df.grch37_ea.values[0]
            r.grch38_ea = df.grch38_ea.values[0]
            r.grch37_alt = df.grch38_alt.values[0]
            r.grch38_alt = df.grch38_alt.values[0]
            r.set_jpn_disease = 'yes'
        else:
            print(r)
            print(df)
            print()
        return r
    df4 = df1.apply(one_row, axis=1)
    return df4

def update_set_field(df1, df2):
    df3 = df2[df2.status == 'present']
    def one_row(r):
        df = df2[(df2.rs_id == r.rs_id) & (df2.grch37_alt == r.grch37_alt) & (df2.grch37_ea == r.grch37_ea) & (df2.grch38_alt == r.grch38_alt) & (df2.grch38_ea == r.grch38_ea)]
        if df.empty:
            pass
        elif df.shape[0] == 1:
            r.set_jpn_disease = 'yes'
        else:
            raise ValueError()
        return r
    df4 = df1.apply(one_row, axis=1)
    return df4

def main(madb_file, madb2_file):
    df1 = pd.read_csv(madb_file)
    df2 = pd.read_csv(madb2_file)

    df3 = check_status(df1, df2)

    df4 = append_absent_data(df1, df3)
    df5 = fill_missing_ea(df4, df3)
    df6 = update_set_field(df5, df3)

    if any(df6.duplicated(['grch37_chr', 'grch37_pos', 'grch37_ref', 'grch37_alt', 'grch37_ea'])):
        raise ValueError()

    if any(df6.duplicated(['grch38_chr', 'grch38_pos', 'grch38_ref', 'grch38_alt', 'grch38_ea'])):
        raise ValueError()
        
    df6.to_csv('update-madb-3.csv', index=False)

if __name__ == '__main__':
    main(*sys.argv[1:])