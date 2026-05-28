import pandas as pd

def load_clean_data():
    df = pd.read_csv("zomato.csv", encoding="latin1")

    # Clean rating
    df = df[df['Aggregate rating'].notnull()]
    df['Aggregate rating'] = pd.to_numeric(df['Aggregate rating'], errors='coerce')

    # Rename columns (IMPORTANT)
    df.rename(columns={
        'Aggregate rating': 'rating',
        'Average Cost for two': 'cost',
        'Votes': 'votes',
        'Cuisines': 'cuisines',
        'City': 'city',
        'Latitude': 'latitude',
        'Longitude': 'longitude'
    }, inplace=True)

    # Clean numeric cost
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce')

    return df