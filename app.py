import os
import subprocess
import sys
import streamlit as st
import pandas as pd
import plotly.express as px

from utils.data_loader import load_data
from utils.recommender import recommend_restaurants
from utils.ai_chat import ai_response
from utils.feature_engineering import prepare_features
from utils.kafka_producer import send_event, get_kafka_producer
from models.rating_model import train_model
from streaming.event_simulator import generate_event


def safe_read_text(path):
    try:
        with open(path, "rb") as f:
            raw = f.read()

        if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
            return raw.decode("utf-16", errors="replace")
        if raw.startswith(b"\xef\xbb\xbf"):
            return raw.decode("utf-8", errors="replace")
        try:
            return raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return raw.decode("latin-1", errors="replace")
    except Exception as exc:
        return f"Unable to read {path}: {exc}"


def extract_spark_output(raw):
    if not raw:
        return ""
    lines = raw.splitlines()
    start = 0
    for i, line in enumerate(lines):
        if "=== Top Cities by Average Rating ===" in line:
            start = i
            break
    filtered = lines[start:]
    cleaned = []
    for line in filtered:
        if line.startswith("python : WARNING"):
            continue
        if line.startswith("At line:1 char:1"):
            continue
        if line.startswith("+") and "python spark/spark_sql.py" in line:
            continue
        if line.startswith("SUCCESS: The process"):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)

# ================= CONFIG =================
st.set_page_config(
    page_title="DineVision AI",
    page_icon="🍽️",
    layout="wide"
)

# ================= LOAD DATA =================
df = load_data()
df = prepare_features(df)
model = train_model(df)

# ================= UI STYLE =================
st.markdown("""
<style>
.stApp {
    background: linear-gradient(to right, #0f0f0f, #1a1a1a);
    color: white;
}
.block-container {
    padding: 2rem;
}
</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
st.sidebar.title("🍽 DineVision AI")
page = st.sidebar.radio(
    "Navigation",
    ["Home", "Recommend", "Predict Rating", "AI Chat", "Analytics", "Live Event", "Outputs"]
)

# ================= HOME =================
if page == "Home":
    st.title("🍽 Smart Restaurant Intelligence System")

    c1, c2, c3 = st.columns(3)
    c1.metric("🌍 Cities", df['city'].nunique())
    c2.metric("🍽 Restaurants", len(df))
    c3.metric("⭐ Avg Rating", round(df['rating'].mean(), 2))

    st.subheader("🏆 Top Rated Restaurants")
    st.dataframe(df.sort_values("rating", ascending=False).head(10))

    st.subheader("🔥 Most Popular Restaurants")
    st.dataframe(df.sort_values("votes", ascending=False).head(10))

# ================= RECOMMEND =================
elif page == "Recommend":
    st.title("🍴 Smart Recommendation Engine")

    col1, col2 = st.columns(2)

    city = col1.selectbox("City", df['city'].unique())
    budget = col1.slider("Budget", 100, 5000, 1000)
    min_rating = col2.slider("Minimum Rating", 0.0, 5.0, 3.5)
    cuisine = col2.selectbox("Cuisine", ["All"] + sorted(df['cuisines'].dropna().unique()))

    cuisine = None if cuisine == "All" else cuisine

    results = recommend_restaurants(df, city, budget, min_rating, cuisine)

    st.success(f"Matches Found: {len(results)}")
    st.dataframe(results.head(15))

    send_event("recommendation", {
        "city": city,
        "budget": budget,
        "min_rating": min_rating,
        "results": len(results)
    })

# ================= PREDICT =================
elif page == "Predict Rating":
    st.title("📊 AI Rating Predictor")

    col1, col2 = st.columns(2)

    votes = col1.slider("Votes", 0, 10000, 500)
    cost = col2.slider("Cost", 100, 5000, 500)

    pred = model.predict(pd.DataFrame([[votes, cost]], columns=["votes", "cost"]))[0]

    st.success(f"⭐ Predicted Rating: {pred:.2f}")

    send_event("rating_prediction", {
        "votes": votes,
        "cost": cost,
        "prediction": float(pred)
    })

# ================= AI CHAT =================
elif page == "AI Chat":
    st.title("🤖 AI Assistant")

    q = st.text_input("Ask anything (best, cheap, city, cuisine...)")

    if q:
        ans = ai_response(q, df)
        st.success(ans)

        send_event("chat_query", {
            "query": q,
            "response": ans
        })

# ================= ANALYTICS =================
elif page == "Analytics":
    st.title("📊 Business Insights Dashboard")

    c1, c2, c3 = st.columns(3)

    c1.metric("Top City", df.groupby('city')['rating'].mean().idxmax())
    c2.metric("Common Cuisine", df['cuisines'].mode()[0])
    c3.metric("Max Rating", df['rating'].max())

    st.markdown("---")

    fig1 = px.bar(df['city'].value_counts().head(10), title="Top Cities")
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = px.pie(df, names='city', title="City Distribution")
    st.plotly_chart(fig2, use_container_width=True)

    fig3 = px.scatter(df, x="cost", y="rating", title="Cost vs Rating")
    st.plotly_chart(fig3, use_container_width=True)

# ================= LIVE EVENT =================
elif page == "Live Event":
    st.title("⚡ Kafka Live Event Generator")

    col1, col2 = st.columns(2)

    city = col1.text_input("City")
    rating = col2.slider("Rating", 0.0, 5.0, 3.5)
    cost = col1.number_input("Cost", 100, 5000, 500)

    if st.button("Send Event"):
        send_event("recommendation", {
            "city": city,
            "rating": rating,
            "cost": cost
        })

        st.success("🚀 Event sent to Kafka successfully!")


# ================= OUTPUTS =================
elif page == "Outputs":
    st.title("📊 Project Outputs")

    st.subheader("Spark SQL Output")
    if st.button("Refresh Spark SQL Output"):
        env = os.environ.copy()
        env['SPARK_HOME'] = r"C:\spark-4.1.1-bin-hadoop3"
        env['PYSPARK_PYTHON'] = sys.executable
        env['PYSPARK_DRIVER_PYTHON'] = sys.executable
        subprocess.run(
            [sys.executable, "spark/spark_sql.py"],
            stdout=open("spark_sql_output.txt", "w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            check=False,
            env=env
        )

    if os.path.exists("spark_sql_output.txt"):
        raw_text = safe_read_text("spark_sql_output.txt")
        parsed = extract_spark_output(raw_text)
        if parsed:
            st.code(parsed)
        else:
            st.text(raw_text)
    else:
        st.warning("spark_sql_output.txt not found. Run python spark/spark_sql.py to generate it.")

    st.markdown("---")
    st.subheader("Spark ML Output")
    spark_ml_path = "models/spark_rating_model"
    if os.path.exists(spark_ml_path):
        file_count = sum(len(files) for _, _, files in os.walk(spark_ml_path))
        st.write(f"Saved Spark ML model directory found: `{spark_ml_path}`")
        st.write(f"Model artifact file count: {file_count}")
        st.write("These artifacts indicate Spark ML training output exists.")
    else:
        st.warning("Spark ML model artifacts not found. Run python spark/spark_ml.py.")

    st.markdown("---")
    st.subheader("Kafka / Event Output")
    producer = get_kafka_producer()
    if producer is not None:
        st.success("Kafka broker reachable via utils/kafka_producer.py")
    else:
        st.error("Kafka broker unreachable or Kafka is disabled. Check KAFKA_BOOTSTRAP_SERVERS and KAFKA_ENABLED.")

    st.write("Sample generated events from streaming/event_simulator.py:")
    sample_events = [generate_event() for _ in range(5)]
    st.json(sample_events)

    st.markdown("---")
    st.subheader("Streaming Graph Output")
    if st.button("Run Streaming Graph Analysis"):
        subprocess.run(
            [sys.executable, "streaming/streaming_graph.py"],
            stdout=open("streaming_graph_output.txt", "w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            check=False,
        )

    if os.path.exists("streaming_graph_output.txt"):
        raw_text = safe_read_text("streaming_graph_output.txt")
        if raw_text:
            st.code(raw_text)
        else:
            st.text("No output generated yet.")
    else:
        st.warning("streaming_graph_output.txt not found. Click 'Run Streaming Graph Analysis' to generate it.")

    if os.path.exists("streaming_graph_output.png"):
        st.image("streaming_graph_output.png", caption="Streaming Graph Analysis")
    else:
        st.warning("streaming_graph_output.png not found. Run streaming/streaming_graph.py to generate it.")

    st.markdown("---")
    st.subheader("Spark Streaming / Graph Output")
    st.write("These components read Kafka events and build streaming analytics:")
    st.write("- spark/spark_streaming.py")
    st.write("- streaming/streaming_graph.py")
    st.write("- services/kafka_consumer.py")
    st.write("To see live streaming output, run these scripts while Kafka is running.")
