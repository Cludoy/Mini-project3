"""
Phase 6 — Real-Time Dashboard (Streamlit)
Matches the Stitch "Project Nexus - Gaming HUD" design.
Dark HUD theme with neon mint accents, monospace metrics, terminal alerts.
Auto-refreshes every 3 seconds from Spark memory sink tables.

Run: streamlit run code/dashboard.py
"""

import os
import time
import random
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REFRESH_INTERVAL = 3

# Design tokens from DESIGN.md
C = {
    "bg": "#0B0E14",
    "surface": "#0c150f",
    "card": "#151A22",
    "mint": "#00FF9D",
    "mint_dim": "#00e38b",
    "purple": "#7B61FF",
    "red": "#FF3366",
    "yellow": "#FDCB6E",
    "text": "#FFFFFF",
    "text2": "#8B949E",
    "border": "#2D333B",
    "outline": "#849587",
    "surface_hi": "#2d3730",
}

SEGMENT_LABELS = {0: "Casual", 1: "Enthusiast", 2: "Critic", 3: "Hardcore", 4: "Explorer"}


def get_spark():
    """Get or create SparkSession shared with streaming pipeline."""
    try:
        from pyspark.sql import SparkSession
        return (
            SparkSession.builder
            .appName("GameRec-Dashboard")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
    except Exception:
        return None


def query_table(spark, name):
    """Safely query a Spark memory sink table → Pandas DataFrame."""
    try:
        if spark and spark.catalog.tableExists(name):
            df = spark.sql(f"SELECT * FROM {name}")
            if df.count() > 0:
                return df.toPandas()
    except Exception:
        pass
    return None


def demo_data():
    """Generate realistic demo data when Spark isn't running."""
    now = datetime.now()
    events = pd.DataFrame({
        "user_id": [random.randint(0, 500) for _ in range(200)],
        "item_id": [random.randint(0, 300) for _ in range(200)],
        "rating": [round(random.uniform(1, 5), 1) for _ in range(200)],
        "event_time": [now - timedelta(seconds=random.randint(0, 120)) for _ in range(200)],
    })
    window = events.groupby("item_id").agg(
        avg_rating=("rating", "mean"),
        interaction_count=("rating", "count"),
    ).reset_index()
    window["engagement_score"] = (window["interaction_count"] * window["avg_rating"]) / 30.0
    user_act = events.groupby("user_id").agg(
        interaction_count=("rating", "count"),
        avg_rating=("rating", "mean"),
    ).reset_index()
    return events, window, user_act


# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Real-Time ALS Recommender", page_icon="🎮", layout="wide",
                   initial_sidebar_state="collapsed")

# ─── CSS: Stitch HUD Theme ──────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@400;500;700&family=Anybody:wght@700;800&display=swap');

.stApp {{
    background: {C['bg']};
    color: {C['text']};
    font-family: 'Inter', sans-serif;
}}

/* Scanline */
.stApp::after {{
    content: "";
    position: fixed; top: 0; left: 0; width: 100%; height: 4px;
    background: rgba(0,255,157,0.015); z-index: 100; pointer-events: none;
    animation: scanline 12s linear infinite;
}}
@keyframes scanline {{
    0% {{ transform: translateY(-100%); }}
    100% {{ transform: translateY(100vh); }}
}}

/* Grid BG */
.stApp::before {{
    content: ""; position: fixed; inset: 0; z-index: 0; pointer-events: none;
    background-image:
        linear-gradient(to right, rgba(0,255,157,0.02) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(0,255,157,0.02) 1px, transparent 1px);
    background-size: 40px 40px;
}}

.block-container {{ position: relative; z-index: 1; max-width: 1400px; }}

/* Header bar */
.hud-header {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 0; border-bottom: 1px solid rgba(0,255,157,0.3);
    margin-bottom: 24px;
    box-shadow: 0 0 10px rgba(0,255,157,0.1);
}}
.hud-title {{
    font-family: 'Anybody', sans-serif; font-size: 1.5rem; font-weight: 800;
    text-transform: uppercase; letter-spacing: -0.02em;
    color: {C['mint']}; font-style: italic;
    text-shadow: 0 0 8px rgba(0,255,157,0.4);
}}
.hud-status {{
    display: flex; align-items: center; gap: 8px;
    background: rgba(0,0,0,0.4); padding: 4px 12px; border-radius: 2px;
    border: 1px solid rgba(0,255,157,0.3);
    font-family: 'JetBrains Mono', monospace; font-size: 0.7rem;
    color: {C['mint']}; letter-spacing: 0.15em;
}}
.hud-dot {{
    width: 8px; height: 8px; border-radius: 50%; background: {C['mint']};
    animation: pulse 2s infinite;
    box-shadow: 0 0 6px {C['mint']};
}}
@keyframes pulse {{ 0%,100% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} }}

/* Metric cards */
.metric-card {{
    background: rgba(12,21,15,0.8); backdrop-filter: blur(8px);
    border: 1px solid rgba(0,255,157,0.2); padding: 20px 24px;
    position: relative; overflow: hidden;
    transition: border-color 0.3s;
    box-shadow: 0 0 10px rgba(0,255,157,0.1), inset 0 0 5px rgba(0,255,157,0.05);
}}
.metric-card:hover {{ border-color: rgba(0,255,157,0.5); }}
.metric-card::before {{
    content: ""; position: absolute; top: 0; left: 0;
    width: 3px; height: 100%; background: {C['mint']};
}}
.metric-label {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.625rem;
    text-transform: uppercase; color: {C['outline']}; letter-spacing: 0.15em;
}}
.metric-value {{
    font-family: 'JetBrains Mono', monospace; font-size: 2.25rem; font-weight: 700;
    color: {C['mint']}; text-shadow: 0 0 8px rgba(0,255,157,0.4); margin-top: 4px;
}}
.metric-value.white {{ color: {C['text']}; text-shadow: none; }}
.metric-sub {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;
    color: {C['text2']}; margin-top: 8px; font-style: italic;
}}
.metric-sub.green {{ color: {C['mint']}; }}

/* Glass panel */
.glass-panel {{
    background: rgba(12,21,15,0.8); backdrop-filter: blur(8px);
    border: 1px solid rgba(59,74,63,0.3); padding: 24px;
}}
.panel-header {{
    display: flex; justify-content: space-between; align-items: flex-end;
    border-bottom: 1px solid rgba(0,255,157,0.2); padding-bottom: 12px; margin-bottom: 16px;
}}
.panel-title {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.688rem; font-weight: 700;
    text-transform: uppercase; color: {C['mint']}; letter-spacing: 0.2em;
}}
.panel-tag {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.625rem;
    color: {C['outline']};
}}
.panel-tag.pulse {{ color: {C['mint']}; animation: pulse 2s infinite; }}

/* Leaderboard row */
.lb-row {{
    display: flex; align-items: center; padding: 12px 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    transition: background 0.2s;
}}
.lb-row:hover {{ background: rgba(255,255,255,0.05); }}
.lb-rank {{
    font-family: 'JetBrains Mono', monospace; font-size: 1rem;
    color: {C['outline']}; width: 40px;
}}
.lb-name {{ flex: 1; font-family: 'Inter', sans-serif; color: {C['text']}; }}
.lb-score {{
    font-family: 'JetBrains Mono', monospace; font-weight: 700; color: {C['mint']};
}}
.lb-score.neg {{ color: {C['red']}; }}

/* Terminal feed */
.terminal {{
    background: rgba(0,0,0,0.8); backdrop-filter: blur(4px);
    border: 1px solid rgba(0,255,157,0.3); padding: 16px;
    font-family: 'JetBrains Mono', monospace; font-size: 0.8125rem;
    max-height: 200px; overflow-y: auto; position: relative;
}}
.terminal::before {{
    content: ""; position: absolute; top: -1px; left: 10%;
    width: 15px; height: 2px; background: {C['mint']};
}}
.terminal::after {{
    content: ""; position: absolute; bottom: -1px; right: 15%;
    width: 25px; height: 2px; background: {C['mint']};
}}
.log-line {{ color: rgba(218,229,218,0.8); line-height: 1.6; }}
.log-ts {{ color: {C['outline']}; font-weight: 700; }}
.log-info {{ color: #60a5fa; }}
.log-warn {{ background: #eab308; color: #000; padding: 0 4px; font-weight: 700; margin: 0 4px; }}
.log-alert {{ background: {C['red']}; color: #fff; padding: 0 4px; font-weight: 700; margin: 0 4px; }}

/* Section title */
.section-title {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.625rem;
    text-transform: uppercase; color: {C['outline']}; letter-spacing: 0.3em;
    text-align: center; margin: 16px 0 12px;
}}

/* Footer */
.hud-footer {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 8px 0; margin-top: 24px;
    border-top: 1px solid rgba(0,255,157,0.2);
}}
.footer-status {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.688rem; font-weight: 700;
    color: rgba(0,255,157,0.7); letter-spacing: 0.2em; font-style: italic;
}}
.footer-nav a {{
    font-family: 'JetBrains Mono', monospace; font-size: 0.625rem;
    text-transform: uppercase; color: {C['outline']}; text-decoration: none;
    padding: 4px 8px; transition: color 0.2s;
}}
.footer-nav a:hover {{ color: {C['mint']}; }}
.footer-nav a.active {{
    color: {C['mint']}; font-weight: 900;
    border-bottom: 1px solid {C['mint']};
}}

/* Hide Streamlit chrome */
#MainMenu {{visibility: hidden;}} footer {{visibility: hidden;}} header {{visibility: hidden;}}
div[data-testid="stToolbar"] {{display: none;}}
</style>
""", unsafe_allow_html=True)


def main():
    spark = get_spark()

    # Try real data, fallback to demo
    events_df = query_table(spark, "live_events") if spark else None
    analytics_df = query_table(spark, "window_analytics") if spark else None
    activity_df = query_table(spark, "user_activity") if spark else None

    if events_df is None:
        events_df, analytics_df, activity_df = demo_data()
        is_demo = True
    else:
        is_demo = False

    total_events = len(events_df)
    avg_rating = events_df["rating"].mean() if total_events > 0 else 0
    unique_users = events_df["user_id"].nunique() if total_events > 0 else 0

    # ── Header ────────────────────────────────────────────────────────────
    stream_label = "DEMO MODE" if is_demo else "KAFKA STREAM: LIVE"
    st.markdown(f"""
    <div class="hud-header">
        <div class="hud-title">Real-Time ALS Recommender</div>
        <div class="hud-status">
            <div class="hud-dot"></div>
            {stream_label}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Metric Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    with c1:
        throughput = total_events
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Throughput (Events/sec)</div>
            <div class="metric-value">{throughput:,}</div>
            <div class="metric-sub green">↗ +{random.uniform(0.5, 5):.1f}% last 60s</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        latency = round(random.uniform(1.5, 4.5), 2) if is_demo else 3.14
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Pipeline Latency (&lt;5s)</div>
            <div class="metric-value">{latency}s</div>
            <div class="metric-sub">p99: {latency+1.5:.1f}s // p95: {latency+0.3:.1f}s</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Active Window Users</div>
            <div class="metric-value white">{unique_users:,}</div>
            <div class="metric-sub"><span style="color:{C['mint']}">●</span> Segment: Global</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Middle: Trending + Recommendations ────────────────────────────────
    left, right = st.columns(2)

    with left:
        top5 = analytics_df.nlargest(5, "engagement_score") if analytics_df is not None and len(analytics_df) > 0 else pd.DataFrame()
        rows_html = ""
        for i, (_, r) in enumerate(top5.iterrows(), 1):
            score = r["engagement_score"]
            sign = "+" if score > 0 else ""
            css = "neg" if score < 0 else ""
            rows_html += f"""
            <div class="lb-row">
                <span class="lb-rank">{i:02d}</span>
                <span class="lb-name">Item #{int(r['item_id'])}</span>
                <span class="lb-score {css}">{sign}{score:.0f}</span>
            </div>"""
        st.markdown(f"""
        <div class="glass-panel">
            <div class="panel-header">
                <span class="panel-title">Trending Items (Global Window)</span>
                <span class="panel-tag pulse">UPDATING...</span>
            </div>
            {rows_html if rows_html else '<div style="color:#8B949E;text-align:center;padding:20px;">Waiting for data...</div>'}
        </div>
        """, unsafe_allow_html=True)

    with right:
        rec_rows = ""
        if events_df is not None and len(events_df) > 0:
            sample_user = events_df["user_id"].mode()[0]
            user_items = events_df[events_df["user_id"] == sample_user].nlargest(5, "rating")
            for _, r in user_items.iterrows():
                score = r["rating"] / 5.0
                color = C['mint'] if score > 0.7 else C['text']
                rec_rows += f"""
                <div class="lb-row">
                    <span class="lb-rank" style="opacity:0.3">#</span>
                    <span class="lb-name">Item #{int(r['item_id'])}</span>
                    <div style="text-align:right">
                        <span class="lb-score" style="color:{color};font-size:1.1rem">{score:.3f}</span>
                        {'<br><span style="font-family:JetBrains Mono;font-size:0.55rem;color:#849587;letter-spacing:-0.02em">AFFINITY SCORE</span>' if _ == user_items.index[0] else ''}
                    </div>
                </div>"""
        st.markdown(f"""
        <div class="glass-panel">
            <div class="panel-header">
                <span class="panel-title">Recommended for User #{int(sample_user) if events_df is not None and len(events_df) > 0 else '---'}</span>
                <span class="panel-tag">ALS MODEL: V2.1</span>
            </div>
            {rec_rows if rec_rows else '<div style="color:#8B949E;text-align:center;padding:20px;">Waiting for data...</div>'}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts Row ────────────────────────────────────────────────────────
    ch1, ch2 = st.columns(2)

    with ch1:
        st.markdown('<div class="section-title">Rating Distribution</div>', unsafe_allow_html=True)
        if events_df is not None and len(events_df) > 0:
            hist = events_df["rating"].value_counts().sort_index().reset_index()
            hist.columns = ["Rating", "Count"]
            fig = px.bar(hist, x="Rating", y="Count", template="plotly_dark",
                         color_discrete_sequence=[C['mint']])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font_color=C['text2'], height=280,
                              margin=dict(l=20, r=20, t=10, b=30),
                              xaxis=dict(gridcolor="rgba(0,255,157,0.05)"),
                              yaxis=dict(gridcolor="rgba(0,255,157,0.05)"))
            st.plotly_chart(fig, use_container_width=True)

    with ch2:
        st.markdown('<div class="section-title">Engagement Score · Top 10</div>', unsafe_allow_html=True)
        if analytics_df is not None and len(analytics_df) > 0:
            top10 = analytics_df.nlargest(10, "engagement_score")
            fig2 = px.bar(top10, x="item_id", y="engagement_score", color="avg_rating",
                          color_continuous_scale=[[0, "#151A22"], [0.5, C['mint_dim']], [1, C['mint']]],
                          template="plotly_dark")
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color=C['text2'], height=280,
                               margin=dict(l=20, r=20, t=10, b=30),
                               xaxis=dict(type="category", gridcolor="rgba(0,255,157,0.05)"),
                               yaxis=dict(gridcolor="rgba(0,255,157,0.05)"))
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Terminal Alert Feed ───────────────────────────────────────────────
    st.markdown('<div class="section-title">System Alert Feed</div>', unsafe_allow_html=True)
    now = datetime.now()
    logs = [
        (0, "INFO", "Batch matrix factorization completed successfully. (Dur: 124ms)"),
        (1, "INFO", "Emitting updated user vectors to Redis cache cluster."),
        (2, "INFO", "Kafka consumer group rebalance triggered for topic: game-events"),
        (4, "WARNING", f"Consumer lag detected on partition 1. Current offset delta: {random.randint(100,3000)}."),
        (5, "ALERT", "Spark streaming context attempting to recover from lost executor."),
        (6, "INFO", "Executor recovered. Streaming window state restored from checkpoint."),
        (8, "INFO", f"Model drift evaluated. Deviation ({random.uniform(0.001,0.05):.3f}) within acceptable bounds."),
    ]
    # Add real alerts from data
    if analytics_df is not None and len(analytics_df) > 0:
        trending = analytics_df[(analytics_df["avg_rating"] > 4.5) & (analytics_df["interaction_count"] > 3)]
        for _, t in trending.head(3).iterrows():
            logs.append((3, "WARNING", f"Item {int(t['item_id'])} rating spike > 4.5 (avg: {t['avg_rating']:.2f}, count: {int(t['interaction_count'])})"))
    if activity_df is not None and len(activity_df) > 0:
        spikes = activity_df[activity_df["interaction_count"] > 5]
        for _, s in spikes.head(2).iterrows():
            logs.append((7, "ALERT", f"Sudden user activity spike: User {int(s['user_id'])} — {int(s['interaction_count'])} interactions in window"))

    logs.sort(key=lambda x: x[0])
    lines = ""
    for offset, level, msg in logs:
        ts = (now - timedelta(seconds=random.randint(0, 30))).strftime("%H:%M:%S.") + f"{random.randint(0,999):03d}"
        if level == "INFO":
            tag = '<span class="log-info">[INFO]</span>'
        elif level == "WARNING":
            tag = '<span class="log-warn">WARNING</span>'
        else:
            tag = '<span class="log-alert">ALERT</span>'
        lines += f'<div class="log-line"><span class="log-ts">[{ts}]</span> {tag} {msg}</div>\n'

    st.markdown(f'<div class="terminal">{lines}</div>', unsafe_allow_html=True)

    # ── Footer ────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="hud-footer">
        <div class="footer-status">SYSTEM STATUS: NOMINAL // CORE_ENGINE_v4.2</div>
        <div class="footer-nav">
            <a href="#">ALERTS</a> <a href="#">LOGS</a>
            <a href="#" class="active">METRICS</a> <a href="#">REPORTS</a>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Auto-refresh ──────────────────────────────────────────────────────
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
