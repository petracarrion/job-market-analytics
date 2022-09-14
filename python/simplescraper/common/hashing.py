import hashlib


def hash_columns(df, cols):
    column = df[cols].apply(lambda row: ';'.join(row.values.astype(str)), axis=1)
    column = column.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    return column
