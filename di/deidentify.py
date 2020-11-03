from assets.mapping import colmap

def simple_di(data):
    std_columns = [col.lower() for col in colmap]
    mapped_columns = [col for col in data if col.lower() in std_columns and col != 'guid']
    return data.drop(mapped_columns, axis=1)