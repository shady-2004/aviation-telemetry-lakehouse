
from __future__ import annotations

import os
from typing import Optional

import altair as alt
import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

# ---------------------------------------------------------------------------
# 0. Page Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Aviation Telemetry",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# 1. Design System — Linear / GitHub Dark Palette
# ---------------------------------------------------------------------------
_CUSTOM_CSS = """
<style>
/* Base canvas & typography */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0d1117 !important;
    color: #e6edf3 !important;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif;
}

/* Sidebar surface */
[data-testid="stSidebar"] {
    background-color: #161b22 !important;
    border-right: 1px solid #30363d !important;
}

/* Metric summary cards */
div[data-testid="stMetric"] {
    background-color: #161b22 !important;
    border: 1px solid #30363d !important;
    border-radius: 6px !important;
    padding: 14px 18px !important;
    box-shadow: none !important;
}

div[data-testid="stMetric"] label {
    color: #8b949e !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #e6edf3 !important;
    font-weight: 600 !important;
    font-size: 1.45rem !important;
    letter-spacing: -0.02em !important;
}

/* Navigation tabs */
button[data-baseweb="tab"] {
    color: #8b949e !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    padding: 8px 16px !important;
}

button[data-baseweb="tab"][aria-selected="true"] {
    color: #e6edf3 !important;
    border-bottom: 2px solid #388bfd !important;
}

/* Map container integration */
[data-testid="stDeckGlJsonChart"] {
    border: 1px solid #30363d;
    border-radius: 6px;
    overflow: hidden;
}

/* Dataframe & inputs */
[data-testid="stDataFrame"] {
    border: 1px solid #30363d;
    border-radius: 6px;
    overflow: hidden;
}

hr {
    border-color: #30363d !important;
}
</style>
"""
st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# 2. MinIO / DuckDB Infrastructure Config
# ---------------------------------------------------------------------------
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BUCKET: str = "aviation-lakehouse"
FCT_TELEMETRY_PATH: str = f"s3://{BUCKET}/fct_flight_telemetry/*/*.parquet"
FCT_DAILY_ROUTE_PATH: str = f"s3://{BUCKET}/fct_daily_route_summary.parquet"
DIM_ROUTES_PATH: str = f"s3://{BUCKET}/dim_routes.parquet"
DIM_AIRLINES_PATH: str = f"s3://{BUCKET}/dim_airlines.parquet"
DIM_AIRPORTS_PATH: str = f"s3://{BUCKET}/dim_airports.parquet"


@st.cache_resource(show_spinner=False)
def _get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Return a configured DuckDB connection with httpfs + MinIO credentials."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3,
            KEY_ID   '{MINIO_ACCESS_KEY}',
            SECRET   '{MINIO_SECRET_KEY}',
            ENDPOINT '{MINIO_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL  false
        );
        """
    )
    return con


def _query(sql: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> pd.DataFrame:
    """Execute SQL query against DuckDB and return DataFrame."""
    if con is None:
        con = _get_duckdb_connection()
    return con.execute(sql).fetchdf()


# ---------------------------------------------------------------------------
# 3. Telemetry Data Loaders (Tab 1)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60, show_spinner=False)
def load_telemetry(
    flight_status: list[str],
    airlines: list[str],
    alt_min: int,
    alt_max: int,
    limit: int,
) -> pd.DataFrame:
    """Load filtered flight telemetry records from gold mart."""
    where_clauses: list[str] = []

    if flight_status and len(flight_status) < 2:
        if "Airborne" in flight_status:
            where_clauses.append("is_on_ground = false")
        else:
            where_clauses.append("is_on_ground = true")

    if airlines:
        quoted = ", ".join(f"'{a}'" for a in airlines)
        where_clauses.append(f"airline_icao_code IN ({quoted})")

    where_clauses.append(f"baro_altitude_feet BETWEEN {alt_min} AND {alt_max}")
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
        SELECT
            flight_ping_hk,
            callsign_code,
            airline_icao_code,
            icao_address,
            squawk_code,
            flight_phase,
            emergency_status,
            latitude,
            longitude,
            baro_altitude_feet,
            geo_altitude_feet,
            velocity_knots,
            vertical_rate_fpm,
            heading_degrees,
            is_on_ground,
            position_timestamp
        FROM read_parquet('{FCT_TELEMETRY_PATH}', hive_partitioning=true)
        WHERE {where_sql}
            AND latitude  IS NOT NULL
            AND longitude IS NOT NULL
        ORDER BY position_timestamp DESC
        LIMIT {limit}
    """
    return _query(sql)


@st.cache_data(ttl=120, show_spinner=False)
def load_airline_codes() -> list[str]:
    """Retrieve distinct airline ICAO codes from telemetry mart."""
    sql = f"""
        SELECT DISTINCT airline_icao_code
        FROM read_parquet('{FCT_TELEMETRY_PATH}', hive_partitioning=true)
        WHERE airline_icao_code IS NOT NULL
            AND airline_icao_code != ''
        ORDER BY airline_icao_code
    """
    df = _query(sql)
    return df["airline_icao_code"].tolist()


# ---------------------------------------------------------------------------
# 4. Commercial & Network Intelligence Data Loaders (Tab 2)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def load_network_kpis() -> dict:
    """Executive KPIs from dim_routes: corridor count, carrier count, direct ratio, codeshare share."""
    sql = f"""
        SELECT
            count(DISTINCT source_airport_code || '->' || destination_airport_code) AS active_corridors,
            count(DISTINCT airline_code) AS operating_airlines,
            round(100.0 * count(CASE WHEN stop_count = 0 THEN 1 END) / nullif(count(*), 0), 1) AS direct_route_pct,
            round(100.0 * count(CASE WHEN is_codeshare THEN 1 END) / nullif(count(*), 0), 1) AS codeshare_pct
        FROM read_parquet('{DIM_ROUTES_PATH}')
    """
    row = _query(sql)
    return row.iloc[0].to_dict() if not row.empty else {}


@st.cache_data(ttl=300, show_spinner=False)
def load_top_corridors(limit: int = 10) -> pd.DataFrame:
    """Top N busiest route corridors by aggregated flight volume."""
    sql = f"""
        SELECT
            r.source_airport_code || ' -> ' || r.destination_airport_code AS corridor,
            r.origin_airport_name,
            r.destination_airport_name,
            sum(f.total_flights_operated) AS total_flights
        FROM read_parquet('{FCT_DAILY_ROUTE_PATH}') f
        INNER JOIN read_parquet('{DIM_ROUTES_PATH}') r
            ON f.route_hk = r.route_hk
        GROUP BY 1, 2, 3
        ORDER BY total_flights DESC
        LIMIT {limit}
    """
    return _query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_hub_volumes(limit: int = 15) -> pd.DataFrame:
    """Departure and arrival volumes for top airport hubs."""
    sql = f"""
        WITH departures AS (
            SELECT
                f.source_airport_code AS airport_code,
                sum(f.total_flights_operated) AS departure_volume
            FROM read_parquet('{FCT_DAILY_ROUTE_PATH}') f
            GROUP BY 1
        ),
        arrivals AS (
            SELECT
                f.destination_airport_code AS airport_code,
                sum(f.total_flights_operated) AS arrival_volume
            FROM read_parquet('{FCT_DAILY_ROUTE_PATH}') f
            GROUP BY 1
        )
        SELECT
            coalesce(d.airport_code, a.airport_code) AS airport_code,
            coalesce(d.departure_volume, 0) AS departures,
            coalesce(a.arrival_volume, 0) AS arrivals,
            coalesce(d.departure_volume, 0) + coalesce(a.arrival_volume, 0) AS total_movements
        FROM departures d
        FULL OUTER JOIN arrivals a ON d.airport_code = a.airport_code
        ORDER BY total_movements DESC
        LIMIT {limit}
    """
    return _query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_airline_market_share(limit: int = 10) -> pd.DataFrame:
    """Top airlines by total flights operated across network."""
    sql = f"""
        SELECT
            coalesce(al.airline_name, f.airline_code) AS airline_name,
            f.airline_code,
            sum(f.total_flights_operated) AS total_flights
        FROM read_parquet('{FCT_DAILY_ROUTE_PATH}') f
        LEFT JOIN read_parquet('{DIM_AIRLINES_PATH}') al
            ON f.airline_code = al.icao_code
        WHERE f.airline_code IS NOT NULL
        GROUP BY 1, 2
        ORDER BY total_flights DESC
        LIMIT {limit}
    """
    df = _query(sql)
    if not df.empty:
        grand_total = df["total_flights"].sum()
        df["pct_share"] = (df["total_flights"] / grand_total * 100.0).round(1) if grand_total > 0 else 0.0
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_equipment_distribution(limit: int = 12) -> pd.DataFrame:
    """Fleet / equipment type distribution across the route network."""
    sql = f"""
        SELECT
            r.equipment_codes,
            count(*) AS route_count
        FROM read_parquet('{DIM_ROUTES_PATH}') r
        WHERE r.equipment_codes IS NOT NULL
            AND r.equipment_codes != ''
        GROUP BY 1
        ORDER BY route_count DESC
        LIMIT {limit}
    """
    return _query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_flight_phase_distribution() -> pd.DataFrame:
    """Flight phase distribution from telemetry fact table (pushdown aggregation)."""
    sql = f"""
        SELECT
            flight_phase,
            count(*) AS record_count
        FROM read_parquet('{FCT_TELEMETRY_PATH}', hive_partitioning=true)
        WHERE flight_phase IS NOT NULL
        GROUP BY 1
        ORDER BY record_count DESC
    """
    return _query(sql)


# ---------------------------------------------------------------------------
# 5. Altitude Color Encoding (Refined Gradient)
# ---------------------------------------------------------------------------

def _altitude_color(alt_ft: float) -> list[int]:
    """Map altitude to a restrained Linear/GitHub dark theme palette.

    Deep teal (surface) -> slate cyan (climb) -> soft amber -> warm gold (cruise).
    """
    if pd.isna(alt_ft) or alt_ft <= 0:
        return [38, 166, 154, 180]
    elif alt_ft < 6_000:
        t = alt_ft / 6_000
        return [
            int(38 + t * (56 - 38)),
            int(166 + t * (139 - 166)),
            int(154 + t * (253 - 154)),
            200,
        ]
    elif alt_ft < 18_000:
        t = (alt_ft - 6_000) / 12_000
        return [
            int(56 + t * (227 - 56)),
            int(139 + t * (179 - 139)),
            int(253 + t * (65 - 253)),
            220,
        ]
    elif alt_ft < 32_000:
        t = (alt_ft - 18_000) / 14_000
        return [
            int(227 + t * (240 - 227)),
            int(179 + t * (192 - 179)),
            int(65 + t * (90 - 65)),
            230,
        ]
    else:
        return [240, 192, 90, 240]


def _apply_colors(df: pd.DataFrame) -> pd.DataFrame:
    """Append RGBA visual encoding column."""
    df_colored = df.copy()
    df_colored["_color"] = df_colored["baro_altitude_feet"].apply(_altitude_color)
    return df_colored


# ---------------------------------------------------------------------------
# 6. Shared Altair Theme Configuration
# ---------------------------------------------------------------------------

_AXIS_CONFIG = dict(
    labelColor="#8b949e",
    titleColor="#8b949e",
    gridColor="#21262d",
    domainColor="#30363d",
)

_LEGEND_CONFIG = dict(
    labelColor="#8b949e",
    titleColor="#8b949e",
)

ACCENT = "#388bfd"
ACCENT_SECONDARY = "#2ea043"
ACCENT_AMBER = "#e3b341"


def _section_heading(text: str) -> None:
    """Render a consistent section heading label."""
    st.markdown(
        f"<div style='font-size:0.85rem; font-weight:600; color:#e6edf3; margin-bottom:8px;'>{text}</div>",
        unsafe_allow_html=True,
    )


def _empty_state(msg: str) -> None:
    """Render an informational empty-state message."""
    st.markdown(
        f"<div style='color:#8b949e; font-size:0.82rem; padding:24px 0;'>{msg}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# 7. Sidebar Controls & Filtering
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="padding: 0.5rem 0 1rem 0;">
            <div style="font-size: 0.95rem; font-weight: 600; color: #e6edf3;">Filters & Parameters</div>
            <div style="font-size: 0.75rem; color: #8b949e;">Telemetry stream criteria</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Refresh Data", width="stretch", type="primary"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

    flight_status: list[str] = st.multiselect(
        "Flight Status",
        options=["Airborne", "On Ground"],
        default=["Airborne", "On Ground"],
    )

    try:
        available_airlines = load_airline_codes()
    except Exception:
        available_airlines = []

    selected_airlines: list[str] = st.multiselect(
        "Airline ICAO",
        options=available_airlines,
        default=[],
        placeholder="All carriers",
    )

    alt_range: tuple[int, int] = st.slider(
        "Altitude Window (ft)",
        min_value=0,
        max_value=45_000,
        value=(0, 45_000),
        step=500,
        format="%d ft",
    )

    ping_limit: int = st.slider(
        "Sample Limit",
        min_value=500,
        max_value=10_000,
        value=5_000,
        step=500,
        help="Volume limit of records retrieved for map rendering.",
    )

    st.markdown("---")
    st.markdown(
        """
        <div style="font-size: 0.72rem; color: #8b949e; line-height: 1.5;">
            <div><strong>Store:</strong> MinIO / S3</div>
            <div><strong>Engine:</strong> DuckDB <code>httpfs</code></div>
            <div><strong>Format:</strong> Apache Parquet</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# 8. Page Header
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div style="padding: 0.25rem 0 1.25rem 0; border-bottom: 1px solid #30363d; margin-bottom: 1.25rem;">
        <div style="display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 0.75rem;">
            <div>
                <h1 style="color: #e6edf3; font-size: 1.4rem; font-weight: 600; margin: 0; letter-spacing: -0.02em;">
                    Aviation Telemetry
                </h1>
            </div>
            <div style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 0.74rem; color: #8b949e; background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 4px 10px;">
                Source: MinIO Gold Marts (Parquet) &nbsp;|&nbsp; Engine: DuckDB &nbsp;|&nbsp; Partition: date_key
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ===========================================================================
# PRIMARY TAB LAYOUT
# ===========================================================================

tab_ops, tab_network = st.tabs([
    "Airspace Operations",
    "Commercial & Network Intelligence",
])


# ===========================================================================
# TAB 1: AIRSPACE OPERATIONS
# ===========================================================================

with tab_ops:

    # --- Load telemetry data -----------------------------------------------
    try:
        df = load_telemetry(
            flight_status=flight_status,
            airlines=selected_airlines,
            alt_min=alt_range[0],
            alt_max=alt_range[1],
            limit=ping_limit,
        )
    except Exception as exc:
        st.error(
            f"Unable to query telemetry mart at {MINIO_ENDPOINT}/{BUCKET}.\n\n"
            f"Error details: {exc}"
        )
        df = pd.DataFrame()

    if df.empty:
        st.info("No flight telemetry records found matching the active filter criteria.")
    else:

        # --- Operational KPI summary row -----------------------------------
        total_aircraft: int = int(df["callsign_code"].nunique())
        airborne_count: int = int((~df["is_on_ground"]).sum())
        total_records: int = len(df)
        airborne_ratio: float = (airborne_count / total_records * 100.0) if total_records > 0 else 0.0
        mean_alt: float = float(df["baro_altitude_feet"].mean())
        avg_speed: float = float(df["velocity_knots"].mean())
        monitored_corridors: int = int(df["airline_icao_code"].nunique())

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Active Aircraft", f"{total_aircraft:,}")
        k2.metric("Airborne Ratio", f"{airborne_ratio:.1f}%")
        k3.metric("Mean Altitude", f"{mean_alt:,.0f} ft" if pd.notna(mean_alt) else "\u2014")
        k4.metric("Ground Speed (avg)", f"{avg_speed:,.0f} kts" if pd.notna(avg_speed) else "\u2014")
        k5.metric("Monitored Corridors", f"{monitored_corridors:,}")

        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

        # --- Geospatial map ------------------------------------------------
        df_map = _apply_colors(df)
        avg_lat: float = float(df_map["latitude"].mean())
        avg_lon: float = float(df_map["longitude"].mean())

        view_state = pdk.ViewState(
            latitude=avg_lat,
            longitude=avg_lon,
            zoom=4,
            pitch=0,
            bearing=0,
        )

        scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map,
            get_position=["longitude", "latitude"],
            get_radius="baro_altitude_feet / 12 + 600",
            get_fill_color="_color",
            pickable=True,
            opacity=0.85,
            radius_min_pixels=3,
            radius_max_pixels=14,
        )

        tooltip = {
            "html": (
                "<div style='font-family:ui-monospace,SFMono-Regular,\"SF Mono\",Menlo,Consolas,monospace; "
                "font-size:12px; line-height:1.5; padding:8px 12px; "
                "background:#161b22; border:1px solid #30363d; border-radius:6px; color:#e6edf3; "
                "box-shadow:0 8px 24px rgba(0,0,0,0.5);'>"
                "<div><span style='color:#8b949e;'>Callsign:</span> <span style='font-weight:600; color:#388bfd;'>{callsign_code}</span></div>"
                "<div><span style='color:#8b949e;'>ICAO:</span> <span>{airline_icao_code}</span></div>"
                "<div><span style='color:#8b949e;'>Altitude:</span> <span>{baro_altitude_feet} ft</span></div>"
                "<div><span style='color:#8b949e;'>Speed:</span> <span>{velocity_knots} kts</span></div>"
                "</div>"
            ),
            "style": {"backgroundColor": "transparent", "color": "white"},
        }

        st.pydeck_chart(
            pdk.Deck(
                layers=[scatter_layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_provider="carto",
                map_style="dark",
            ),
            width="stretch",
            height=540,
        )

        # Altitude legend
        st.markdown(
            """
            <div style='display: flex; align-items: center; gap: 14px; margin-top: 6px; margin-bottom: 24px; font-size: 0.74rem; color: #8b949e;'>
                <span style='display: flex; align-items: center; gap: 5px;'>
                    <span style='display: inline-block; width: 10px; height: 10px; border-radius: 2px; background: #26a69a;'></span>
                    Surface / Low (&lt;6,000 ft)
                </span>
                <span style='display: flex; align-items: center; gap: 5px;'>
                    <span style='display: inline-block; width: 10px; height: 10px; border-radius: 2px; background: #388bfd;'></span>
                    Climb / Approach
                </span>
                <span style='display: flex; align-items: center; gap: 5px;'>
                    <span style='display: inline-block; width: 10px; height: 10px; border-radius: 2px; background: #e3b341;'></span>
                    Intermediate
                </span>
                <span style='display: flex; align-items: center; gap: 5px;'>
                    <span style='display: inline-block; width: 10px; height: 10px; border-radius: 2px; background: #f0c05a;'></span>
                    Cruise (&gt;30,000 ft)
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # --- Analytics sub-tabs --------------------------------------------
        sub_tab1, sub_tab2, sub_tab3 = st.tabs([
            "Telemetry Distribution",
            "Carrier Distribution",
            "Telemetry Log",
        ])

        with sub_tab1:
            col_hist, col_scatter = st.columns(2)

            with col_hist:
                _section_heading("Altitude Profile (feet)")
                alt_chart = (
                    alt.Chart(df)
                    .mark_bar(
                        cornerRadiusTopLeft=2,
                        cornerRadiusTopRight=2,
                        color=ACCENT,
                        opacity=0.85,
                    )
                    .encode(
                        alt.X("baro_altitude_feet:Q", bin=alt.Bin(maxbins=35), title="Barometric Altitude (ft)"),
                        alt.Y("count():Q", title="Record Count"),
                        tooltip=[
                            alt.Tooltip("baro_altitude_feet:Q", title="Altitude (ft)", format=",.0f"),
                            alt.Tooltip("count():Q", title="Records", format=","),
                        ],
                    )
                    .properties(height=340)
                    .configure_view(strokeWidth=0)
                    .configure_axis(**_AXIS_CONFIG)
                )
                st.altair_chart(alt_chart, width="stretch")

            with col_scatter:
                _section_heading("Speed vs. Altitude Correlation")
                scatter_sample = df.sample(min(len(df), 2000))
                scatter_chart = (
                    alt.Chart(scatter_sample)
                    .mark_circle(size=24, opacity=0.6)
                    .encode(
                        alt.X("velocity_knots:Q", title="Velocity (kts)"),
                        alt.Y("baro_altitude_feet:Q", title="Barometric Altitude (ft)"),
                        alt.Color(
                            "flight_phase:N",
                            scale=alt.Scale(
                                domain=["cruising", "climbing", "descending", "level_flight", "ground", "unknown"],
                                range=[ACCENT, ACCENT_SECONDARY, ACCENT_AMBER, "#db61a2", "#8b949e", "#6e7681"],
                            ),
                            title="Flight Phase",
                        ),
                        tooltip=[
                            alt.Tooltip("callsign_code:N", title="Callsign"),
                            alt.Tooltip("velocity_knots:Q", title="Velocity (kts)", format=",.0f"),
                            alt.Tooltip("baro_altitude_feet:Q", title="Altitude (ft)", format=",.0f"),
                            alt.Tooltip("flight_phase:N", title="Phase"),
                        ],
                    )
                    .properties(height=340)
                    .configure_view(strokeWidth=0)
                    .configure_axis(**_AXIS_CONFIG)
                    .configure_legend(**_LEGEND_CONFIG)
                )
                st.altair_chart(scatter_chart, width="stretch")

        with sub_tab2:
            _section_heading("Top Carriers by Telemetry Volume")
            carrier_counts = (
                df.groupby("airline_icao_code", as_index=False)
                .size()
                .rename(columns={"size": "record_count"})
                .sort_values("record_count", ascending=False)
                .head(20)
            )

            if carrier_counts.empty:
                _empty_state("No carrier data available for the current selection.")
            else:
                carrier_chart = (
                    alt.Chart(carrier_counts)
                    .mark_bar(
                        cornerRadiusTopLeft=3,
                        cornerRadiusTopRight=3,
                        color=ACCENT,
                        opacity=0.9,
                    )
                    .encode(
                        alt.X("airline_icao_code:N", sort="-y", title="Carrier ICAO", axis=alt.Axis(labelAngle=-45)),
                        alt.Y("record_count:Q", title="Record Volume"),
                        tooltip=[
                            alt.Tooltip("airline_icao_code:N", title="Carrier"),
                            alt.Tooltip("record_count:Q", title="Records", format=","),
                        ],
                    )
                    .properties(height=380)
                    .configure_view(strokeWidth=0)
                    .configure_axis(**_AXIS_CONFIG)
                )
                st.altair_chart(carrier_chart, width="stretch")

        with sub_tab3:
            _section_heading("Fact Stream Records")

            log_cols = [
                "callsign_code", "airline_icao_code", "icao_address", "flight_phase",
                "baro_altitude_feet", "velocity_knots", "heading_degrees",
                "vertical_rate_fpm", "latitude", "longitude", "is_on_ground",
                "position_timestamp",
            ]
            log_cols = [c for c in log_cols if c in df.columns]

            search_query: str = st.text_input(
                "Filter by callsign or ICAO address",
                placeholder="Filter records...",
                label_visibility="collapsed",
            )

            df_log = df[log_cols].copy()
            if search_query:
                mask = (
                    df_log["callsign_code"].astype(str).str.contains(search_query, case=False, na=False)
                    | df_log["icao_address"].astype(str).str.contains(search_query, case=False, na=False)
                )
                df_log = df_log[mask]

            st.dataframe(
                df_log,
                width="stretch",
                height=420,
                column_config={
                    "callsign_code": st.column_config.TextColumn("Callsign"),
                    "airline_icao_code": st.column_config.TextColumn("Carrier"),
                    "baro_altitude_feet": st.column_config.NumberColumn("Altitude (ft)", format="%,.0f"),
                    "velocity_knots": st.column_config.NumberColumn("Speed (kts)", format="%,.0f"),
                    "heading_degrees": st.column_config.NumberColumn("Heading (\u00b0)", format="%.0f"),
                    "vertical_rate_fpm": st.column_config.NumberColumn("V/S (fpm)", format="%+,.0f"),
                    "position_timestamp": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
                },
            )

            st.download_button(
                label="Export CSV",
                data=df_log.to_csv(index=False).encode("utf-8"),
                file_name="telemetry_stream.csv",
                mime="text/csv",
                width="stretch",
            )


# ===========================================================================
# TAB 2: COMMERCIAL & NETWORK INTELLIGENCE
# ===========================================================================

with tab_network:

    # --- A. Executive KPI Row ----------------------------------------------
    try:
        kpis = load_network_kpis()
    except Exception:
        kpis = {}

    if kpis:
        nk1, nk2, nk3, nk4 = st.columns(4)
        nk1.metric("Active Commercial Corridors", f"{kpis.get('active_corridors', 0):,}")
        nk2.metric("Operating Airline Count", f"{kpis.get('operating_airlines', 0):,}")
        nk3.metric("Direct Route Ratio", f"{kpis.get('direct_route_pct', 0):.1f}%")
        nk4.metric("Codeshare Share", f"{kpis.get('codeshare_pct', 0):.1f}%")
    else:
        _empty_state("Commercial network KPIs unavailable. Ensure dim_routes has been materialized in MinIO.")

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    # --- B. Top Flight Corridors & Hub Volume ------------------------------
    col_corridors, col_hubs = st.columns(2)

    with col_corridors:
        _section_heading("Top 10 Busiest Route Corridors")
        try:
            df_corridors = load_top_corridors(limit=10)
        except Exception:
            df_corridors = pd.DataFrame()

        if df_corridors.empty:
            _empty_state("No route corridor data available. Ensure fct_daily_route_summary and dim_routes have been materialized.")
        else:
            corridors_chart = (
                alt.Chart(df_corridors)
                .mark_bar(
                    cornerRadiusBottomRight=3,
                    cornerRadiusTopRight=3,
                    color=ACCENT,
                    opacity=0.9,
                )
                .encode(
                    alt.Y("corridor:N", sort="-x", title=None),
                    alt.X("total_flights:Q", title="Total Flights"),
                    tooltip=[
                        alt.Tooltip("corridor:N", title="Route"),
                        alt.Tooltip("origin_airport_name:N", title="Origin"),
                        alt.Tooltip("destination_airport_name:N", title="Destination"),
                        alt.Tooltip("total_flights:Q", title="Flights", format=","),
                    ],
                )
                .properties(height=360)
                .configure_view(strokeWidth=0)
                .configure_axis(**_AXIS_CONFIG)
            )
            st.altair_chart(corridors_chart, width="stretch")

    with col_hubs:
        _section_heading("Top Airport Hubs by Movement Volume")
        try:
            df_hubs = load_hub_volumes(limit=15)
        except Exception:
            df_hubs = pd.DataFrame()

        if df_hubs.empty:
            _empty_state("No hub volume data available. Ensure fct_daily_route_summary has been materialized.")
        else:
            # Melt departures/arrivals into a stacked format
            df_hubs_long = df_hubs.melt(
                id_vars=["airport_code", "total_movements"],
                value_vars=["departures", "arrivals"],
                var_name="direction",
                value_name="volume",
            )
            hub_chart = (
                alt.Chart(df_hubs_long)
                .mark_bar(
                    cornerRadiusTopLeft=2,
                    cornerRadiusTopRight=2,
                    opacity=0.9,
                )
                .encode(
                    alt.X("airport_code:N", sort="-y", title="Airport", axis=alt.Axis(labelAngle=-45)),
                    alt.Y("volume:Q", title="Flight Volume", stack="zero"),
                    alt.Color(
                        "direction:N",
                        scale=alt.Scale(domain=["departures", "arrivals"], range=[ACCENT, ACCENT_AMBER]),
                        title="Direction",
                    ),
                    tooltip=[
                        alt.Tooltip("airport_code:N", title="Airport"),
                        alt.Tooltip("direction:N", title="Direction"),
                        alt.Tooltip("volume:Q", title="Volume", format=","),
                    ],
                )
                .properties(height=360)
                .configure_view(strokeWidth=0)
                .configure_axis(**_AXIS_CONFIG)
                .configure_legend(**_LEGEND_CONFIG)
            )
            st.altair_chart(hub_chart, width="stretch")

    st.markdown("---")

    # --- C. Airline Market Share & Equipment Distribution -------------------
    col_market, col_equip = st.columns(2)

    with col_market:
        _section_heading("Airline Market Share (Top 10)")
        try:
            df_market = load_airline_market_share(limit=10)
        except Exception:
            df_market = pd.DataFrame()

        if df_market.empty:
            _empty_state("No airline market share data available. Ensure fct_daily_route_summary and dim_airlines have been materialized.")
        else:
            market_chart = (
                alt.Chart(df_market)
                .mark_bar(
                    cornerRadiusBottomRight=3,
                    cornerRadiusTopRight=3,
                    color=ACCENT,
                    opacity=0.9,
                )
                .encode(
                    alt.Y("airline_name:N", sort="-x", title=None),
                    alt.X("total_flights:Q", title="Total Flights Operated"),
                    tooltip=[
                        alt.Tooltip("airline_name:N", title="Airline"),
                        alt.Tooltip("airline_code:N", title="ICAO"),
                        alt.Tooltip("total_flights:Q", title="Flights", format=","),
                        alt.Tooltip("pct_share:Q", title="Share (%)", format=".1f"),
                    ],
                )
                .properties(height=360)
                .configure_view(strokeWidth=0)
                .configure_axis(**_AXIS_CONFIG)
            )
            st.altair_chart(market_chart, width="stretch")

    with col_equip:
        _section_heading("Equipment & Fleet Distribution")
        try:
            df_equip = load_equipment_distribution(limit=12)
        except Exception:
            df_equip = pd.DataFrame()

        if df_equip.empty:
            _empty_state("No equipment data available. Ensure dim_routes has been materialized with equipment_codes populated.")
        else:
            equip_chart = (
                alt.Chart(df_equip)
                .mark_bar(
                    cornerRadiusTopLeft=2,
                    cornerRadiusTopRight=2,
                    color=ACCENT_AMBER,
                    opacity=0.9,
                )
                .encode(
                    alt.X("equipment_codes:N", sort="-y", title="Equipment Type", axis=alt.Axis(labelAngle=-45)),
                    alt.Y("route_count:Q", title="Route Count"),
                    tooltip=[
                        alt.Tooltip("equipment_codes:N", title="Equipment"),
                        alt.Tooltip("route_count:Q", title="Routes", format=","),
                    ],
                )
                .properties(height=360)
                .configure_view(strokeWidth=0)
                .configure_axis(**_AXIS_CONFIG)
            )
            st.altair_chart(equip_chart, width="stretch")

    st.markdown("---")

    # --- D. Flight Phase Distribution (Telemetry-derived) ------------------
    _section_heading("Flight Phase Distribution (ADS-B Telemetry)")
    try:
        df_phases = load_flight_phase_distribution()
    except Exception:
        df_phases = pd.DataFrame()

    if df_phases.empty:
        _empty_state("No flight phase data available. Ensure fct_flight_telemetry has been materialized.")
    else:
        total_phase_records = df_phases["record_count"].sum()
        df_phases["pct"] = (df_phases["record_count"] / total_phase_records * 100.0).round(1)

        # Phase label display ordering
        phase_display_order = ["ground", "climbing", "cruising", "descending", "level_flight", "unknown"]
        phase_color_map = {
            "ground": "#8b949e",
            "climbing": ACCENT_SECONDARY,
            "cruising": ACCENT,
            "descending": ACCENT_AMBER,
            "level_flight": "#db61a2",
            "unknown": "#6e7681",
        }

        # Metric cards for key phases
        phase_cols = st.columns(len(df_phases))
        for i, (_, row) in enumerate(df_phases.iterrows()):
            with phase_cols[i % len(phase_cols)]:
                phase_label = str(row["flight_phase"]).replace("_", " ").title()
                st.metric(phase_label, f"{row['record_count']:,}", delta=f"{row['pct']:.1f}%")

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

        # Horizontal bar chart
        phase_chart = (
            alt.Chart(df_phases)
            .mark_bar(
                cornerRadiusBottomRight=3,
                cornerRadiusTopRight=3,
                opacity=0.9,
            )
            .encode(
                alt.Y(
                    "flight_phase:N",
                    sort=phase_display_order,
                    title=None,
                ),
                alt.X("record_count:Q", title="Telemetry Records"),
                alt.Color(
                    "flight_phase:N",
                    scale=alt.Scale(
                        domain=list(phase_color_map.keys()),
                        range=list(phase_color_map.values()),
                    ),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("flight_phase:N", title="Phase"),
                    alt.Tooltip("record_count:Q", title="Records", format=","),
                    alt.Tooltip("pct:Q", title="Share (%)", format=".1f"),
                ],
            )
            .properties(height=220)
            .configure_view(strokeWidth=0)
            .configure_axis(**_AXIS_CONFIG)
        )
        st.altair_chart(phase_chart, width="stretch")


# ---------------------------------------------------------------------------
# Footer Status
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div style='text-align: center; color: #6e7681; font-size: 0.72rem; margin-top: 2.5rem; padding: 1rem 0; border-top: 1px solid #21262d;'>
        Aviation Telemetry Lakehouse &nbsp;&bull;&nbsp; MinIO &rarr; DuckDB &rarr; Streamlit &nbsp;&bull;&nbsp; 60s Telemetry / 300s Network Cache TTL
    </div>
    """,
    unsafe_allow_html=True,
)
