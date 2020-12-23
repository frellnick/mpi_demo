import pandas as pd 

import logging

utillogger = logging.getLogger(__name__)

def compare_in(colname, searchlist):
    cond1 = colname in searchlist
    cond2 = '_'.join(colname.split('_')[1:]) in searchlist
    cond3 = sum([colname in item for item in searchlist]) > 0
    return sum([cond1, cond2, cond3]) > 0


def union_frames(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat([t1, t2], ignore_index=True)
    utillogger.info(f"Union Frames successful created dataframe of length {len(df)}")
    return df


def get_column_intersect(dfa: pd.DataFrame, dfb: pd.DataFrame) -> list:
    try:
        assert len(dfa.columns) > 0
        assert len(dfb.columns) > 0
        c1 = set(dfa.columns)
        c2 = set(dfb.columns)
        return list(c1.intersection(c2))
    except AssertionError as e:
        utillogger.info(f"Could not find intersection. {e}")
        return []


def match_dataframe_columns(t1: pd.DataFrame, t2: pd.DataFrame) -> tuple:
    keep = get_column_intersect(t1, t2)
    return t1[keep], t2[keep]


def result_proxy_to_dataframe(resultproxy):
    d, a = {}, []
    for rowproxy in resultproxy:
        # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in rowproxy.items():
            # build up the dictionary
            d = {**d, **{column: value}}
        a.append(d)
    return pd.DataFrame.from_records(a)


def match_dtype(df1: pd.DataFrame, df2: pd.DataFrame):
    valid_columns = get_column_intersect(df1, df2)

    df1 = extract_dataframe(df1)
    df2 = extract_dataframe(df2)

    for row in df1[valid_columns].dtypes.iteritems():
        col = row[0]
        if col in df1.columns:
            try:
                df2[col] = df2[col].astype(row[1])
                utillogger.info(f"Column {col} attempting cast to {row[1]}")
            except ValueError:
                utillogger.info(f"Casting of ID Column {col} failed.  Attempting pd.to_numeric.")
                df2[col] = pd.to_numeric(df2[col])
                df1[col] = pd.to_numeric(df1[col].astype(float))
                utillogger.info(f"Data.Type {df1[col].dtype}; ID.Type {df2[col].dtype}")
            except Exception as e:
                utillogger.error(f'Could not cast columns {col}\n{e}')
    return df1, df2


def extract_dataframe(v) -> pd.DataFrame:
    """
    Extract Dataframe
        Tries to extract data attribute from object.  Assists in 
        working directly with View types.
    """

    try:
        return v.data 
    except:
        return v