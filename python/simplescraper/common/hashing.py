import hashlib


def hash_str(value):
    return hashlib.sha256(value.encode()).hexdigest()


def hash_columns(df, cols):
    column = df[cols].apply(lambda row: ';'.join(row.values.astype(str)), axis=1)
    column = column.apply(hash_str)
    return column
