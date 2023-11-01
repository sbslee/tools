import sys
import pandas as pd

def parse_ucsc_output(file, assembly):
    df = pd.read_csv(file)
    if df.shape[1] != 17:
        raise ValueError()
    for x in ['hap', 'fix', 'alt']:    
        df = df[~df['#chrom'].str.contains(x)]
    df['alts'] = df['alts'].str.strip(',')
    df = df[['class', 'name', '#chrom', 'chromEnd', 'ref', 'alts']]
    df.columns = ['type', 'rs_id', f'{assembly}_chr', f'{assembly}_pos', f'{assembly}_ref', f'{assembly}_alt']
    if not df[df.duplicated('rs_id')].empty:
        raise ValueError()
    df = df.set_index('rs_id')
    return df

def prepare_madb_table(grch37_file, grch38_file, output_file):
    df1 = parse_ucsc_output(grch37_file, 'grch37')
    df2 = parse_ucsc_output(grch38_file, 'grch38')
    df3 = df1.merge(df2, left_index=True, right_index=True)
    if not (df1.shape[0] == df2.shape[0] == df3.shape[0]):
        raise ValueError()
    if not all(df3.type_x == df3.type_y):
        raise ValueError()
    df3 = df3.drop(columns='type_y')
    df3 = df3.rename(columns={'type_x': 'type'})
    df3.to_csv(output_file)

if __name__ == '__main__':
    prepare_madb_table(*sys.argv[1:])