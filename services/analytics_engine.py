def process_event(event):
    if not isinstance(event, dict):
        raise ValueError("event must be a dict")

    event_type = str(event.get("event", "")).lower()

    if event_type == "recommendation":
        city = event.get("city", "unknown city")
        results = event.get("results")
        if results is not None:
            return f"Update trending restaurants: {results} recommendations for {city}"
        return f"Update trending restaurants for {city}"

    if event_type == "chat_query":
        query = event.get("query", "unknown query")
        return f"Update popular queries: '{query}'"

    if event_type == "rating_prediction":
        prediction = event.get("prediction") or event.get("predicted_rating")
        if isinstance(prediction, (int, float)):
            return f"Update ML stats: predicted rating = {prediction:.2f}"
        return "Update ML stats"

    return f"Unhandled analytics event type: {event_type}"


if __name__ == "__main__":
    sample_events = [
        {
            "event": "recommendation",
            "city": "Hyderabad",
            "results": 12
        },
        {
            "event": "chat_query",
            "query": "best restaurants near me"
        },
        {
            "event": "rating_prediction",
            "prediction": 4.3
        },
        {
            "event": "unknown_event"
        }
    ]

    for evt in sample_events:
        print(process_event(evt))
