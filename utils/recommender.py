def recommend_restaurants(df, city, budget, min_rating, cuisine=None):
    filtered = df[
        (df['city'] == city) &
        (df['cost'] <= budget) &
        (df['rating'] >= min_rating)
    ].copy()

    if cuisine:
        filtered = filtered[
            filtered['cuisines'].str.contains(cuisine, na=False)
        ]

    if filtered.empty:
        return filtered

    filtered['score'] = (
        filtered['rating'] * 0.7 +
        filtered['votes'].rank(pct=True) * 0.3
    )

    return filtered.sort_values("score", ascending=False).head(20)