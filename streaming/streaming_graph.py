import os
import pandas as pd
import matplotlib.pyplot as plt

def process_batch(df):
    if df.empty:
        return

    # Simple analysis with pandas
    print("=== Top City -> Cuisine Relationships by Average Rating ===")
    result = df.groupby(['city', 'cuisine'])['rating'].mean().reset_index()
    result = result.sort_values('rating', ascending=False).head(20)
    print(result.to_string(index=False))

    print("=== Cuisine Popularity by Count ===")
    cuisine_count = df['cuisine'].value_counts().reset_index()
    cuisine_count.columns = ['cuisine', 'count']
    cuisine_count = cuisine_count.head(20)
    print(cuisine_count.to_string(index=False))

    # Generate graphs
    plt.figure(figsize=(12, 6))

    # Graph 1: Cuisine Popularity
    plt.subplot(1, 2, 1)
    plt.bar(cuisine_count['cuisine'][:10], cuisine_count['count'][:10])
    plt.title('Top 10 Cuisines by Popularity')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Count')

    # Graph 2: Average Rating by Cuisine
    plt.subplot(1, 2, 2)
    avg_rating = df.groupby('cuisine')['rating'].mean().sort_values(ascending=False).head(10)
    plt.bar(avg_rating.index, avg_rating.values)
    plt.title('Top 10 Cuisines by Average Rating')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Average Rating')

    plt.tight_layout()
    plt.savefig('streaming_graph_output.png')
    print("\nGraphs saved to 'streaming_graph_output.png'")


def main():
    data_path = os.environ.get("ZOMATO_DATA_PATH", "data/zomato.csv")

    df = pd.read_csv(data_path, encoding='latin1')
    df = df.rename(columns={
        'Aggregate rating': 'rating',
        'Average Cost for two': 'cost',
        'Votes': 'votes',
        'City': 'city',
        'Cuisines': 'cuisine'
    })
    df = df.dropna(subset=['rating', 'votes'])

    # Split cuisines if multiple
    df['cuisine'] = df['cuisine'].str.split(', ')
    df = df.explode('cuisine')

    process_batch(df)


if __name__ == "__main__":
    main()
