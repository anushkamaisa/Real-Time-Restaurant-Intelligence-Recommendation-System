def ai_response(query, df):
    q = query.lower()

    if "best" in q:
        return df.sort_values("rating", ascending=False).iloc[0]["name"]

    if "cheap" in q:
        return df.sort_values("cost").iloc[0]["name"]

    if "city" in q:
        return df.groupby("city")["rating"].mean().idxmax()

    if "popular" in q:
        return df.sort_values("votes", ascending=False).iloc[0]["name"]

    return "Try: best / cheap / popular"