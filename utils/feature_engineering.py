def prepare_features(df):
    df = df.copy()

    df['cost_per_vote'] = df['cost'] / (df['votes'] + 1)

    df['rating_weighted'] = df['rating'] * df['votes']

    return df