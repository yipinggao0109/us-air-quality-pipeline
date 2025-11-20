"""
Streamlit-based demo map to explore 2024 US PM2.5 data pulled from OpenAQ.

The layout includes filters, a PM2.5 heatmap view, and monitoring site details
for stakeholders to explore air quality data across the United States.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import numpy as np

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from utils.db_loader import (
    load_measurements as load_measurements_from_db,
    load_sensor_metadata as load_sensor_metadata_from_db,
)
SCENARIO_THRESHOLDS = {
    "Base Case (All Data)": None,
    "AQI Watch (≥35 µg/m³)": 35,
    "Severe Episode (≥55 µg/m³)": 55,
}


@st.cache_data(show_spinner=True)
def load_measurements_cached() -> pd.DataFrame:
    return load_measurements_from_db(parameter="pm25")


@st.cache_data(show_spinner=True)
def load_sensor_metadata_cached() -> pd.DataFrame:
    return load_sensor_metadata_from_db()


def inject_custom_styles():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

        /* Force dark mode colors - always applied */
        :root, [data-theme="light"], [data-theme="dark"], .stApp {
            --bg-primary: #0E1117 !important;
            --bg-secondary: #262730 !important;
            --bg-card: #1E1E2E !important;
            --text-primary: #FAFAFA !important;
            --text-muted: #A0A0A0 !important;
            --border-color: rgba(255, 255, 255, 0.1) !important;
            --accent: #A78BFA !important;
            --accent-strong: #F59E0B !important;
        }

        /* Hide the sidebar (because filters are already on the main page) */
        section[data-testid="stSidebar"] {
            display: none !important;
        }

        /* Main page background */
        .stApp, .main, .block-container {
            background-color: var(--bg-primary) !important;
            color: var(--text-primary) !important;
            font-family: 'Space Grotesk', sans-serif;
        }

        /* Filter panel style */
        [data-testid="column"]:first-child {
            background-color: var(--bg-secondary);
            border-radius: 12px;
            padding: 1.5rem;
            margin-right: 1rem;
        }

        /* Spacing between elements in filter panel */
        [data-testid="column"]:first-child .stRadio,
        [data-testid="column"]:first-child .stDateInput,
        [data-testid="column"]:first-child .stMultiSelect {
            margin-bottom: 1rem;
        }

        /* Sidebar panel content background */
        section[data-testid="stSidebar"] .stMarkdown,
        section[data-testid="stSidebar"] .stRadio,
        section[data-testid="stSidebar"] .stDateInput,
        section[data-testid="stSidebar"] .stMultiSelect {
            background-color: var(--bg-primary) !important;
        }

        .stApp header, .stApp [data-testid="stToolbar"] {
            background: transparent;
        }

        .hero {
            padding: 16px 0 30px;
        }

        .hero-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.4em;
            color: var(--text-muted);
            font-size: 0.75rem;
        }

        .hero h1 {
            font-size: clamp(2.2rem, 3vw, 3.2rem);
            margin: 0.2rem 0;
            color: var(--text-primary);
        }

        .hero p {
            color: var(--text-muted);
            max-width: 720px;
            font-size: 1rem;
        }

        .card-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.3em;
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-bottom: 0.4rem;
        }

        .scenario-note {
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-top: 0.4rem;
        }

        .insight-metric {
            margin-bottom: 18px;
        }

        .metric-label {
            font-size: 0.75rem;
            letter-spacing: 0.25em;
            text-transform: uppercase;
            color: var(--text-muted);
        }

        .metric-value {
            font-size: 2.4rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-top: 0.2rem;
        }

        .metric-helper {
            font-size: 0.85rem;
            color: var(--text-muted);
        }

        .map-card .stPydeckChart {
            height: 520px !important;
        }

        .card-caption {
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-top: 0.6rem;
        }

        .section-label {
            text-transform: uppercase;
            letter-spacing: 0.35em;
            color: var(--text-muted);
            font-size: 0.72rem;
            margin-bottom: 12px;
        }

        /* Radio buttons styling */
        .stRadio > label {
            font-weight: 600;
            color: var(--text-muted);
        }

        div[data-baseweb="radio"] > div {
            background: var(--bg-secondary) !important;
            border-radius: 999px;
            padding: 4px;
        }

        div[data-baseweb="radio"] label {
            border-radius: 999px;
            padding: 6px 16px;
            color: var(--text-primary) !important;
        }

        div[data-baseweb="radio"] input:checked + div {
            background-color: var(--accent) !important;
        }

        /* DataFrames */
        .stDataFrame {
            margin-top: 12px;
            background-color: var(--bg-card) !important;
        }

        /* Input fields */
        .stDateInput input, .stMultiSelect input {
            background-color: var(--bg-secondary) !important;
            color: var(--text-primary) !important;
            border-color: var(--border-color) !important;
        }

        /* Ensure all text is properly colored */
        p, span, div, label {
            color: var(--text-primary);
        }

        /* Fix sidebar text color */
        section[data-testid="stSidebar"] * {
            color: var(--text-primary) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )




def humanize(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.1f}"


def render_metric(label: str, value: str, helper: str = ""):
    helper_html = f"<div class='metric-helper'>{helper}</div>" if helper else ""
    st.markdown(
        f"""
        <div class="insight-metric">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            {helper_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def get_heatmap_color(pm25_value):
    """
    Returns the color for the heatmap based on PM2.5 concentration (green -> yellow -> orange -> red -> purple)
    
    The color steps follow AQI standards:
    - 0-12: Green (Good)
    - 12-35: Yellow (Moderate)
    - 35-55: Orange (Unhealthy for Sensitive Groups)
    - 55-150: Red (Unhealthy)
    - 150+: Purple (Very Unhealthy)
    """
    if pd.isna(pm25_value):
        return [128, 128, 128]  # Gray
    elif pm25_value < 12:
        return [0, 228, 0]      # Green
    elif pm25_value < 35:
        return [255, 255, 0]    # Yellow
    elif pm25_value < 55:
        return [255, 126, 0]    # Orange
    elif pm25_value < 150:
        return [255, 0, 0]      # Red
    else:
        return [143, 63, 151]   # Purple

def render_map(df: pd.DataFrame):
    if df.empty:
        st.info("No rows matched the current filters.")
        return pd.DataFrame()

    # Diagnostic: Show how many rows have latitude/longitude values
    total_rows = len(df)
    has_coords = df[df["latitude"].notna() & df["longitude"].notna()]
    print(f"🔍 Diagnostic: Total rows = {total_rows}, with lat/lon = {len(has_coords)}")
    
    agg = (
        df.groupby(
            ["location_id", "location_name", "latitude", "longitude"],
            dropna=False,
        )
        .agg(
            avg_pm25=("value", "mean"),
            max_pm25=("value", "max"),
            measurements=("value", "count"),
        )
        .reset_index()
    )

    # Diagnostic: Show aggregated data statistics
    print(f"🔍 After aggregation: total locations = {len(agg)}")
    print(f"🔍 Locations with lat/lon = {len(agg[agg['latitude'].notna() & agg['longitude'].notna()])}")
    
    agg = agg.dropna(subset=["latitude", "longitude"])
    
    # Increase radius based on PM2.5 concentration, emphasize dots
    agg["radius"] = agg["avg_pm25"].clip(lower=5) * 2000  # scaled up from 1200 to 2000

    if agg.empty:
        st.warning(f"⚠️ {total_rows:,} rows in the database, but none have latitude/longitude coordinates.")
        st.info("💡 Please verify: 1) Whether data fetch included lat/lon 2) If your latitude/longitude columns contain any data.")
        return pd.DataFrame()  # Return empty DataFrame instead of None

    view_state = pdk.ViewState(
        latitude=float(agg["latitude"].mean()),
        longitude=float(agg["longitude"].mean()),
        zoom=3.5,
        pitch=30,
    )

    # Use JSON serialization/deserialization to ensure native Python types
    agg_json = agg.to_json(orient='records', date_format='iso')
    data = json.loads(agg_json)
    
    # Format values for tooltip display
    for record in data:
        if 'avg_pm25' in record and record['avg_pm25'] is not None:
            record['avg_pm25_display'] = f"{record['avg_pm25']:.1f}"
        else:
            record['avg_pm25_display'] = "N/A"
            
        if 'max_pm25' in record and record['max_pm25'] is not None:
            record['max_pm25_display'] = f"{record['max_pm25']:.1f}"
        else:
            record['max_pm25_display'] = "N/A"

    # Force dark mode for all map elements
    is_dark_mode = True
    
    # Use an expression in the Layer to calculate colors, avoiding serialization issues
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position="[longitude, latitude]",
        get_radius="radius",
        # Use a JavaScript expression for fill color based on avg_pm25
        get_fill_color="""
            avg_pm25 < 12 ? [0, 228, 0] :
            avg_pm25 < 35 ? [255, 255, 0] :
            avg_pm25 < 55 ? [255, 126, 0] :
            avg_pm25 < 150 ? [255, 0, 0] :
            [143, 63, 151]
        """,
        pickable=True,
        opacity=0.85,
    )

    # Dark mode tooltip styling
    tooltip_bg = "#0E1117"
    tooltip_color = "white"
    
    tooltip = {
        "html": "<b>{location_name}</b><br/>"
                "Average PM2.5: {avg_pm25_display} µg/m³<br/>"
                "Maximum PM2.5: {max_pm25_display} µg/m³<br/>"
                "Measurements per day: {measurements}",
        "style": {
            "backgroundColor": tooltip_bg,
            "color": tooltip_color,
            "fontSize": "14px",
            "padding": "10px",
            "borderRadius": "5px",
        },
    }

    # Always use dark map style
    map_style = "dark"
    
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_provider="carto",
        map_style=map_style,
    )

    st.pydeck_chart(deck, use_container_width=True)
    st.markdown(
        "<p class='card-caption'>Color means PM2.5 concentration: 🟢 Good → 🟡 Moderate → 🟠 Unhealthy for Sensitive Groups → 🔴 Unhealthy → 🟣 Very Unhealthy</p>",
        unsafe_allow_html=True,
    )

    return agg.sort_values("avg_pm25", ascending=False)


def render_trend_chart(df: pd.DataFrame, selected_locations: list = None):
    """
    Render a line chart showing PM2.5 trends over time for selected locations.

    Functionality description: (What this function does):
    - render = render/display
    - trend_chart = trend chart

    Purpose: Shows a line chart of PM2.5 values over time for selected stations, maximum 5 at a time for clarity.

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    selected_locations : list
        List of selected location names (maximum 5 shown)

    Returns:
    --------
    None (displays chart in Streamlit)
    """
    if df.empty:
        st.info("No data available for trend chart.")
        return

    # Ensure 'value' column is numeric type
    df_copy = df.copy()
    df_copy["value"] = pd.to_numeric(df_copy["value"], errors='coerce')

    # If locations are selected, filter for them
    if selected_locations and len(selected_locations) > 0:
        trend_df = df_copy[df_copy["location_name"].isin(selected_locations)].copy()
        # Limit to max 5 locations for readability
        if len(selected_locations) > 5:
            top_5_locations = selected_locations[:5]
            trend_df = trend_df[trend_df["location_name"].isin(top_5_locations)]
            st.warning(f"⚠️ Showing only the first 5 selected locations for clarity.")
    else:
        # If no locations selected, show top 5 by average PM2.5
        location_avg = df_copy.groupby("location_name")["value"].mean().sort_values(ascending=False)
        top_5_locations = location_avg.head(5).index.tolist()
        trend_df = df_copy[df_copy["location_name"].isin(top_5_locations)].copy()
        st.info("💡 Showing top 5 locations by average PM2.5. Use the location filter to select specific sites.")

    if trend_df.empty:
        st.info("No data available for the selected locations.")
        return

    # Aggregate by date and location
    daily_trends = (
        trend_df.groupby(["date", "location_name"])
        .agg({"value": "mean"})
        .reset_index()
    )

    # Convert date to datetime for proper plotting
    daily_trends["date"] = pd.to_datetime(daily_trends["date"])

    # Create Altair scatterplot
    chart = alt.Chart(daily_trends).mark_circle(size=60, opacity=0.8).encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%Y-%m-%d")),
        y=alt.Y("value:Q", title="PM2.5 Concentration (µg/m³)", scale=alt.Scale(zero=False)),
        color=alt.Color("location_name:N", title="Location", legend=alt.Legend(
            orient="top",
            direction="horizontal",
            titleAnchor="middle"
        )),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("location_name:N", title="Location"),
            alt.Tooltip("value:Q", title="PM2.5", format=".1f")
        ]
    ).properties(
        height=400,
        title="PM2.5 Daily Trends"
    ).configure_axis(
        labelColor='#9EA2C7',
        titleColor='#9EA2C7',
        gridColor='#262730'
    ).configure_title(
        color='#F6F7FF',
        fontSize=16,
        anchor='start'
    ).configure_legend(
        labelColor='#F6F7FF',
        titleColor='#F6F7FF'
    )

    st.altair_chart(chart, use_container_width=True)


def render_top_polluted_locations(df: pd.DataFrame, top_n: int = 10):
    """
    Render a ranking of the most polluted locations

    Functionality description: (What this function does):
    - render = render/display
    - top_polluted_locations = most polluted location ranking

    Purpose: Shows the ranking of the top N stations with the highest average PM2.5 concentration.

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    top_n : int
        Number of top locations to show (default: 10)
    """
    if df.empty:
        st.info("No data available for ranking.")
        return

    # Ensure 'value' column is numeric type
    df_copy = df.copy()
    df_copy["value"] = pd.to_numeric(df_copy["value"], errors='coerce')

    # Calculate average PM2.5 by location
    top_locations = (
        df_copy.groupby(["location_id", "location_name", "latitude", "longitude"])
        .agg({
            "value": ["mean", "max", "count"]
        })
        .reset_index()
    )

    # Flatten column names
    top_locations.columns = ["location_id", "location_name", "latitude", "longitude", 
                              "avg_pm25", "max_pm25", "measurements"]

    # Sort by average PM2.5
    top_locations = top_locations.sort_values("avg_pm25", ascending=False).head(top_n)

    # Add ranking number
    top_locations.insert(0, "Rank", range(1, len(top_locations) + 1))

    # Add AQI category
    def get_aqi_category(pm25):
        if pm25 < 12:
            return "🟢 Good"
        elif pm25 < 35:
            return "🟡 Moderate"
        elif pm25 < 55:
            return "🟠 Unhealthy (Sensitive)"
        elif pm25 < 150:
            return "🔴 Unhealthy"
        else:
            return "🟣 Very Unhealthy"

    top_locations["AQI Category"] = top_locations["avg_pm25"].apply(get_aqi_category)

    # Format numeric columns
    top_locations["avg_pm25"] = top_locations["avg_pm25"].round(1)
    top_locations["max_pm25"] = top_locations["max_pm25"].round(1)

    # Display ranking
    st.markdown(f"### 🏆 Top {top_n} Most Polluted Locations")
    st.dataframe(
        top_locations[["Rank", "location_name", "avg_pm25", "max_pm25", "AQI Category", "measurements"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "location_name": "Location",
            "avg_pm25": st.column_config.NumberColumn("Avg PM2.5 (µg/m³)", format="%.1f"),
            "max_pm25": st.column_config.NumberColumn("Peak PM2.5 (µg/m³)", format="%.1f"),
            "measurements": "Days Monitored"
        }
    )


def render_aqi_distribution_pie(df: pd.DataFrame):
    """
    Render a pie chart showing the distribution of AQI categories

    Functionality description: (What this function does):
    - render = render/display
    - aqi_distribution = Air Quality Index distribution
    - pie = pie chart

    Purpose: Displays a pie chart showing the percentage of various air quality levels (Good, Moderate, Unhealthy, etc.).

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    """
    if df.empty:
        st.info("No data available for AQI distribution.")
        return

    # Categorize PM2.5 values into AQI levels
    def categorize_aqi(pm25):
        if pd.isna(pm25):
            return "Unknown"
        elif pm25 < 12:
            return "Good (0-12)"
        elif pm25 < 35:
            return "Moderate (12-35)"
        elif pm25 < 55:
            return "Unhealthy for Sensitive (35-55)"
        elif pm25 < 150:
            return "Unhealthy (55-150)"
        else:
            return "Very Unhealthy (150+)"

    aqi_df = df.copy()
    # Ensure 'value' column is numeric type
    aqi_df["value"] = pd.to_numeric(aqi_df["value"], errors='coerce')
    aqi_df["AQI Category"] = aqi_df["value"].apply(categorize_aqi)

    # Count measurements in each category
    aqi_counts = aqi_df["AQI Category"].value_counts().reset_index()
    aqi_counts.columns = ["AQI Category", "Count"]
    aqi_counts["Percentage"] = (aqi_counts["Count"] / aqi_counts["Count"].sum() * 100).round(1)

    # Define color scheme matching AQI standards
    color_scale = alt.Scale(
        domain=[
            "Good (0-12)",
            "Moderate (12-35)",
            "Unhealthy for Sensitive (35-55)",
            "Unhealthy (55-150)",
            "Very Unhealthy (150+)"
        ],
        range=["#00E400", "#FFFF00", "#FF7E00", "#FF0000", "#8F3F97"]
    )

    # Create pie chart using Altair
    chart = alt.Chart(aqi_counts).mark_arc(innerRadius=50).encode(
        theta=alt.Theta("Count:Q"),
        color=alt.Color("AQI Category:N", scale=color_scale, legend=alt.Legend(title="AQI Category")),
        tooltip=[
            alt.Tooltip("AQI Category:N", title="Category"),
            alt.Tooltip("Count:Q", title="Measurements"),
            alt.Tooltip("Percentage:Q", title="Percentage", format=".1f")
        ]
    ).properties(
        width=400,
        height=400,
        title="Air Quality Distribution"
    ).configure_title(
        color='#F6F7FF',
        fontSize=16,
        anchor='start'
    ).configure_legend(
        labelColor='#F6F7FF',
        titleColor='#F6F7FF'
    )

    st.altair_chart(chart, use_container_width=True)

    # Also show summary statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        healthy_pct = aqi_counts[aqi_counts["AQI Category"] == "Good (0-12)"]["Percentage"].sum()
        st.metric("🟢 Good Air Quality", f"{healthy_pct:.1f}%")
    with col2:
        moderate_pct = aqi_counts[aqi_counts["AQI Category"] == "Moderate (12-35)"]["Percentage"].sum()
        st.metric("🟡 Moderate", f"{moderate_pct:.1f}%")
    with col3:
        unhealthy_pct = aqi_counts[
            aqi_counts["AQI Category"].isin([
                "Unhealthy for Sensitive (35-55)",
                "Unhealthy (55-150)",
                "Very Unhealthy (150+)"
            ])
        ]["Percentage"].sum()
        st.metric("🔴 Unhealthy Levels", f"{unhealthy_pct:.1f}%")


def render_extreme_events_alert(df: pd.DataFrame, threshold: float = 150):
    """
    Show alerts for extreme pollution events (PM2.5 >= threshold)

    Functionality description: (What this function does):
    - render = render/display
    - extreme_events = extreme events
    - alert = alert

    Purpose: Identify and display severe pollution events (when PM2.5 exceeds the set threshold).

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    threshold : float
        PM2.5 threshold for extreme events (default: 150 µg/m³)
    """
    # Ensure 'value' column is numeric type
    df_copy = df.copy()
    df_copy["value"] = pd.to_numeric(df_copy["value"], errors='coerce')

    extreme_events = df_copy[df_copy["value"] >= threshold].copy()

    if extreme_events.empty:
        st.success(f"✅ No extreme pollution events (PM2.5 ≥ {threshold} µg/m³) detected in the filtered period.")
        return

    # Count unique locations and dates
    num_locations = extreme_events["location_id"].nunique()
    num_days = extreme_events["date"].nunique()
    max_value = extreme_events["value"].max()

    # Show alert banner
    st.error(f"⚠️ **ALERT**: {num_days} extreme pollution event(s) detected at {num_locations} location(s)!")

    # Show top 10 most severe events
    st.markdown("### 🚨 Most Severe Pollution Events")

    top_events = (
        extreme_events.nlargest(10, "value")
        [["date", "location_name", "value", "latitude", "longitude"]]
        .reset_index(drop=True)
    )

    top_events["value"] = top_events["value"].round(1)
    top_events.insert(0, "Severity", ["🔴 SEVERE" if v >= 200 else "🟠 HIGH" for v in top_events["value"]])

    st.dataframe(
        top_events,
        use_container_width=True,
        hide_index=True,
        column_config={
            "date": "Date",
            "location_name": "Location",
            "value": st.column_config.NumberColumn("PM2.5 (µg/m³)", format="%.1f"),
            "latitude": st.column_config.NumberColumn("Latitude", format="%.4f"),
            "longitude": st.column_config.NumberColumn("Longitude", format="%.4f")
        }
    )


def render_weekday_weekend_comparison(df: pd.DataFrame):
    """
    Compare PM2.5 levels between weekdays and weekends

    Functionality description: (What this function does):
    - render = render/display
    - weekday_weekend_comparison = comparison of weekdays and weekends

    Purpose: Compare PM2.5 concentration differences between weekdays and weekends to analyze the impact of human activity on air quality.

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    """
    if df.empty:
        st.info("No data available for weekday/weekend comparison.")
        return

    # Add day of week column
    df_copy = df.copy()
    # Ensure 'value' column is numeric type
    df_copy["value"] = pd.to_numeric(df_copy["value"], errors='coerce')
    df_copy["datetime_date"] = pd.to_datetime(df_copy["date"])
    df_copy["day_of_week"] = df_copy["datetime_date"].dt.dayofweek  # Monday=0, Sunday=6
    df_copy["day_type"] = df_copy["day_of_week"].apply(
        lambda x: "Weekend" if x >= 5 else "Weekday"
    )

    # Calculate statistics
    comparison = df_copy.groupby("day_type")["value"].agg([
        ("mean", "mean"),
        ("median", "median"),
        ("max", "max"),
        ("count", "count")
    ]).reset_index()

    # Create bar chart
    chart = alt.Chart(comparison).mark_bar(size=100).encode(
        x=alt.X("day_type:N", title="", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("mean:Q", title="Average PM2.5 (µg/m³)"),
        color=alt.Color("day_type:N", 
                       scale=alt.Scale(domain=["Weekday", "Weekend"], 
                                     range=["#FF6B6B", "#4ECDC4"]),
                       legend=None),
        tooltip=[
            alt.Tooltip("day_type:N", title="Day Type"),
            alt.Tooltip("mean:Q", title="Average PM2.5", format=".2f"),
            alt.Tooltip("median:Q", title="Median PM2.5", format=".2f"),
            alt.Tooltip("count:Q", title="Measurements")
        ]
    ).properties(
        height=400,
        title="PM2.5 Comparison: Weekday vs Weekend"
    ).configure_axis(
        labelColor='#9EA2C7',
        titleColor='#9EA2C7',
        gridColor='#262730'
    ).configure_title(
        color='#F6F7FF',
        fontSize=16,
        anchor='start'
    )

    st.altair_chart(chart, use_container_width=True)

    # Show percentage difference
    if len(comparison) == 2:
        weekday_avg = comparison[comparison["day_type"] == "Weekday"]["mean"].values[0]
        weekend_avg = comparison[comparison["day_type"] == "Weekend"]["mean"].values[0]
        diff_pct = ((weekday_avg - weekend_avg) / weekend_avg * 100)

        if abs(diff_pct) > 5:
            if diff_pct > 0:
                st.warning(f"📊 Weekdays have **{diff_pct:.1f}% higher** PM2.5 levels than weekends on average. This suggests traffic and industrial activities significantly impact air quality.")
            else:
                st.info(f"📊 Weekends have **{abs(diff_pct):.1f}% higher** PM2.5 levels than weekdays on average.")
        else:
            st.info(f"📊 PM2.5 levels are similar between weekdays and weekends (difference: {diff_pct:.1f}%).")


def render_seasonal_trends(df: pd.DataFrame):
    """
    Show monthly PM2.5 trends to identify seasonal patterns

    Functionality description: (What this function does):
    - render = render/display
    - seasonal_trends = seasonal trends

    Purpose: Show the monthly variation of PM2.5 to identify seasonal patterns (e.g., winter heating causing increased pollution).

    Parameters:
    -----------
    df : pd.DataFrame
        Filtered measurements DataFrame
    """
    if df.empty:
        st.info("No data available for seasonal analysis.")
        return

    df_copy = df.copy()
    # Ensure 'value' column is numeric type
    df_copy["value"] = pd.to_numeric(df_copy["value"], errors='coerce')
    df_copy["datetime_date"] = pd.to_datetime(df_copy["date"])
    df_copy["month"] = df_copy["datetime_date"].dt.month
    df_copy["month_name"] = df_copy["datetime_date"].dt.strftime("%B")

    # Calculate monthly statistics
    monthly_stats = df_copy.groupby(["month", "month_name"])["value"].agg([
        ("mean", "mean"),
        ("min", "min"),
        ("max", "max")
    ]).reset_index().sort_values("month")

    # Create line chart with area
    base = alt.Chart(monthly_stats).encode(
        x=alt.X("month_name:N", title="Month", sort=[
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ])
    )

    # Line for average
    line = base.mark_line(point=True, color="#F59E0B", size=3).encode(
        y=alt.Y("mean:Q", title="PM2.5 Concentration (µg/m³)"),
        tooltip=[
            alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("mean:Q", title="Average PM2.5", format=".2f"),
            alt.Tooltip("min:Q", title="Min PM2.5", format=".2f"),
            alt.Tooltip("max:Q", title="Max PM2.5", format=".2f")
        ]
    )

    # Area for range
    area = base.mark_area(opacity=0.3, color="#F59E0B").encode(
        y=alt.Y("min:Q", title=""),
        y2="max:Q"
    )

    chart = (area + line).properties(
        height=400,
        title="Monthly PM2.5 Trends (Seasonal Pattern)"
    ).configure_axis(
        labelColor='#9EA2C7',
        titleColor='#9EA2C7',
        gridColor='#262730'
    ).configure_title(
        color='#F6F7FF',
        fontSize=16,
        anchor='start'
    )

    st.altair_chart(chart, use_container_width=True)

    # Find worst and best months
    worst_month = monthly_stats.loc[monthly_stats["mean"].idxmax()]
    best_month = monthly_stats.loc[monthly_stats["mean"].idxmin()]

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "🔴 Worst Month",
            worst_month["month_name"],
            f"{worst_month['mean']:.1f} µg/m³",
            delta_color="inverse"
        )
    with col2:
        st.metric(
            "🟢 Best Month",
            best_month["month_name"],
            f"{best_month['mean']:.1f} µg/m³",
            delta_color="normal"
        )


def main():
    st.set_page_config(
        page_title="US PM2.5 Observatory",
        layout="wide",
        initial_sidebar_state="collapsed",
        menu_items={
            'Get Help': None,
            'Report a bug': None,
            'About': None
        }
    )
    inject_custom_styles()

    st.markdown(
        """
        <div class="hero">
            <div class="hero-eyebrow">National Air Quality Intelligence</div>
            <h1>US PM2.5 Observatory</h1>
            <p>
                Explore daily particulate matter levels across the United States.
                Switch between scenarios, use filters, and review sensor monitoring coverage from Supabase.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    measurements = load_measurements_cached()
    sensor_meta = load_sensor_metadata_cached()

    if measurements.empty:
        st.error("No PM2.5 measurements available in PostgreSQL yet. Please trigger the Airflow DAG to load data.")
        st.stop()

    min_date = measurements["date"].min()
    max_date = measurements["date"].max()
    default_start = pd.to_datetime(min_date).date()
    default_end = pd.to_datetime(max_date).date()

    # Create two-column layout: filters on the left, content on the right
    filter_col, content_col = st.columns([1, 3])

    # Left filter panel
    with filter_col:
        st.markdown("<div class='card-eyebrow'>Scenario</div>", unsafe_allow_html=True)
        scenario_choice = st.radio(
            "Scenario",
            list(SCENARIO_THRESHOLDS.keys()),
            horizontal=False,
            label_visibility="collapsed",
        )
        scenario_threshold = SCENARIO_THRESHOLDS[scenario_choice]
        scenario_text = (
            "Showing all measurements for all locations."
            if scenario_threshold is None
            else f"Highlighting days with PM2.5 ≥ {scenario_threshold} µg/m³."
        )
        st.markdown(f"<div class='scenario-note'>{scenario_text}</div>", unsafe_allow_html=True)

        st.markdown("<div class='card-eyebrow' style='margin-top:1.4rem;'>Filters</div>", unsafe_allow_html=True)
        date_range = st.date_input(
            "Date range",
            value=(default_start, default_end),
            min_value=min_date,
            max_value=max_date,
            key="date_range_input",
        )

        # Handle possible return types from date input
        if isinstance(date_range, tuple) and len(date_range) == 2:
            # The user picked both start and end dates
            start_date, end_date = date_range
        elif isinstance(date_range, tuple) and len(date_range) == 1:
            # Only one date picked
            start_date = date_range[0]
            end_date = default_end
        elif date_range is not None:
            # Single date value
            start_date = date_range
            end_date = date_range
        else:
            # No date chosen, use defaults
            start_date = default_start
            end_date = default_end

        location_options = sensor_meta["location_name"].unique().tolist() if not sensor_meta.empty else []
        selected_locations = st.multiselect(
            "Locations (optional)",
            location_options,
            default=[],
            key="location_filter",
        )

    # Apply filters
    filtered = measurements.copy()

    # Apply the date filter if valid
    if start_date and end_date:
        date_series = pd.to_datetime(filtered["date"]).dt.date
        mask = (date_series >= start_date) & (date_series <= end_date)
        filtered = filtered[mask]
    if selected_locations:
        filtered = filtered[filtered["location_name"].isin(selected_locations)]
    if scenario_threshold is not None:
        filtered = filtered[filtered["value"] >= scenario_threshold]

    # Right panel content
    with content_col:
        # KEY METRICS (top, four columns)
        st.markdown("<div class='card-eyebrow'>Key Metrics</div>", unsafe_allow_html=True)
        metric_cols = st.columns(4)

        with metric_cols[0]:
            render_metric("Locations", humanize(filtered["location_id"].nunique()), "Stations with PM2.5 data")

        with metric_cols[1]:
            render_metric("Average PM2.5", humanize(filtered["value"].mean()), "Daily mean after filters")

        with metric_cols[2]:
            render_metric("Peak PM2.5", humanize(filtered["value"].max()), "Maximum recorded value")

        with metric_cols[3]:
            render_metric("Rows", humanize(len(filtered)), "Daily measurements in view")

        # Map (below the KEY METRICS row)
        st.markdown("<div class='section-label' style='margin-top:2rem;'>National Overview</div>", unsafe_allow_html=True)
        map_data = render_map(filtered)

        # Extreme Events Alert
        # Alert for serious pollution when PM2.5 exceeds 150
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Pollution Alerts</div>", unsafe_allow_html=True)
        render_extreme_events_alert(filtered, threshold=150)

        # Top 10 Polluted Locations
        # Display ranking of the top 10 stations with highest average PM2.5
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Most Polluted Areas</div>", unsafe_allow_html=True)
        render_top_polluted_locations(filtered, top_n=10)

        # Trend Chart
        # Show daily PM2.5 trends over time at selected locations
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Daily PM2.5 Trends</div>", unsafe_allow_html=True)
        render_trend_chart(filtered, selected_locations)

        # AQI Distribution
        # Pie chart breaking down measurements by air quality levels
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Air Quality Distribution</div>", unsafe_allow_html=True)
        render_aqi_distribution_pie(filtered)

        # Weekday vs Weekend
        # Analyze impact of work week vs weekend on air quality
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Weekday vs Weekend Analysis</div>", unsafe_allow_html=True)
        render_weekday_weekend_comparison(filtered)

        # Seasonal Trends
        # Show monthly changes and seasonal pollution pattern
        st.markdown("<div class='section-label' style='margin-top:2rem;'>Seasonal Patterns</div>", unsafe_allow_html=True)
        render_seasonal_trends(filtered)

        # Monitoring stations list
        # Display a detailed table of all monitoring stations
        if not map_data.empty:
            st.markdown("<h4 style='margin-top:2rem;'>Monitoring Sites</h4>", unsafe_allow_html=True)
            st.dataframe(
                map_data,
                use_container_width=True,
                height=min(420, 60 + 30 * len(map_data)),
            )


if __name__ == "__main__":
    main()
