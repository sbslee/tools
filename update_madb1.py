import sys
import pandas as pd
import warnings

def pick_alt_allele(df):
    def one_row(r):
        majors1 = [x for x in r.majorAllele.split(',') if x]
        minors1 = [x for x in r.minorAllele.split(',') if x]
        a1 = max(majors1, key=majors1.count)
        b1 = max(minors1, key=minors1.count)
        majors2 = [x for x in majors1 if x != r.ref]
        minors2 = [x for x in minors1 if x != r.ref]
        if majors2:
            a2 = max(majors2, key=majors2.count)
        else:
            a2 = ''
        if minors2:
            b2 = max(minors2, key=minors2.count)
        else:
            b2 = ''
        if a1 == r.ref and b1 != r.ref:
            r.alts = b1
        elif a1 != r.ref and b1 == r.ref:
            r.alts = a1
        elif a2 == b2 or (a2 and not b2) or (not a2 and b2):
            r.alts = a2
        else:
            raise ValueError()
        return r
    df = df.apply(one_row, axis=1)
    return df

def parse_ucsc_output(file, assembly):
    df = pd.read_csv(file)
    if df.shape[1] != 17:
        raise ValueError()
    for x in ['hap', 'fix', 'alt']:    
        df = df[~df['#chrom'].str.contains(x)]
    df = pick_alt_allele(df)
    df = df[['class', 'name', '#chrom', 'chromEnd', 'ref', 'alts']]
    df.columns = ['type', 'rs_id', f'{assembly}_chr', f'{assembly}_pos', f'{assembly}_ref', f'{assembly}_alt']
    if not df[df.duplicated('rs_id')].empty:
        raise ValueError()
    df = df.set_index('rs_id')
    return df

def check_data(df):
    def one_row(r):
        if set([r.grch37_ref, r.grch37_alt]) != set([r.grch38_ref, r.grch38_alt]):
            warnings.warn(f'{r.name} has different ref/alt between grch37 and grch38')
    df.apply(one_row, axis=1)

def prepare_madb_table(grch37_file, grch38_file):
    df1 = parse_ucsc_output(grch37_file, 'grch37')
    df2 = parse_ucsc_output(grch38_file, 'grch38')
    df3 = df1.merge(df2, left_index=True, right_index=True)
    if not (df1.shape[0] == df2.shape[0] == df3.shape[0]):
        raise ValueError()
    if not all(df3.type_x == df3.type_y):
        raise ValueError()
    df3 = df3.drop(columns='type_y')
    df3 = df3.rename(columns={'type_x': 'type'})
    check_data(df3)
    df3.to_csv('update-madb-1.csv')

if __name__ == '__main__':
    prepare_madb_table(*sys.argv[1:])