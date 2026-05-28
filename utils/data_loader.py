import pandas as pd

def load_data():
    df = pd.read_csv("data/zomato.csv", encoding="latin1")

    df.rename(columns={
        'Aggregate rating': 'rating',
        'Average Cost for two': 'cost',
        'Votes': 'votes',
        'Restaurant Name': 'name',
        'Cuisines': 'cuisines',
        'City': 'city'
    }, inplace=True)

    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
    df['votes'] = pd.to_numeric(df['votes'], errors='coerce')

    return df.dropna()