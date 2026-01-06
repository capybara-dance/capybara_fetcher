import streamlit as st
import requests
import pandas as pd
import io
import os
import json
import altair as alt
import datetime as dt

st.set_page_config(page_title="Korea Stock Feature Cache Inspector", layout="wide")

st.title("📊 Korea Stock Feature Cache Inspector")

# 사이드바 설정
st.sidebar.header("Settings")
# 기본값은 현재 사용자 이름/레포 이름 패턴을 가정하거나 비워둡니다.
# 사용자가 직접 입력하도록 안내하는 것이 가장 확실합니다.
default_repo = "yunu-lee/capybara_fetcher" # 예시 값
repo_name = st.sidebar.text_input("Repository (owner/repo)", value=default_repo) 
github_token = st.sidebar.text_input("GitHub Token (Optional, for private repos)", type="password")

@st.cache_data(ttl=60)
def get_releases(repo, token=None):
    if not repo:
        return []
        
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    url = f"https://api.github.com/repos/{repo}/releases"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            st.error(f"Repository not found: {repo}")
            return []
        else:
            st.error(f"Failed to fetch releases: {response.status_code} {response.reason}")
            return []
    except Exception as e:
        st.error(f"Connection error: {e}")
        return []

@st.cache_data(ttl=300)
def load_parquet_from_url(url, token=None):
    headers = {}
    # Private asset 다운로드 시에는 token 헤더와 Accept 헤더가 필요할 수 있음
    # 하지만 browser_download_url은 보통 Public이면 바로 접근 가능하고,
    # Private이면 API url을 써야 하는데 여기서는 browser_download_url을 사용함.
    # 만약 Private Repo라면 token이 있어도 browser_download_url로 직접 requests.get 하면 404가 뜰 수 있음.
    # (API url: https://api.github.com/repos/:owner/:repo/releases/assets/:asset_id)
    # 복잡성을 피하기 위해 Public Repo 가정이거나, Token이 있으면 시도해봄.
    
    if token:
        headers["Authorization"] = f"token {token}"
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        return pd.read_parquet(io.BytesIO(response.content))
    except Exception as e:
        st.error(f"Error loading parquet: {e}")
        return None

@st.cache_data(ttl=300)
def load_json_from_url(url, token=None):
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        return json.loads(response.content.decode("utf-8"))
    except Exception as e:
        st.error(f"Error loading metadata json: {e}")
        return None

def _ensure_datetime(series: pd.Series) -> pd.Series:
    # Robust conversion for parquet-loaded types (datetime64, date, int timestamp, etc.)
    return pd.to_datetime(series, errors="coerce")

def _pick_default_date_window(dmin: pd.Timestamp, dmax: pd.Timestamp, days: int = 365) -> tuple[pd.Timestamp, pd.Timestamp]:
    if pd.isna(dmin) or pd.isna(dmax):
        return dmin, dmax
    start = max(dmin, dmax - pd.Timedelta(days=days))
    return start, dmax

def _axis_assignment(df: pd.DataFrame, base_col: str, other_cols: list[str]) -> tuple[list[str], list[str]]:
    """
    Heuristic: columns with range far from base go to right axis.
    """
    if base_col not in df.columns:
        return [base_col], other_cols
    base = pd.to_numeric(df[base_col], errors="coerce")
    base_range = float((base.max() - base.min()) if base.notna().any() else 0.0)
    if base_range <= 0:
        return [base_col] + other_cols, []

    left_cols = [base_col]
    right_cols: list[str] = []
    for c in other_cols:
        s = pd.to_numeric(df[c], errors="coerce")
        r = float((s.max() - s.min()) if s.notna().any() else 0.0)
        if r <= 0:
            left_cols.append(c)
            continue
        ratio = r / base_range
        if ratio >= 10 or ratio <= 0.1:
            right_cols.append(c)
        else:
            left_cols.append(c)
    return left_cols, right_cols

def _build_newhigh_marker_layer(
    df: pd.DataFrame,
    date_col: str,
    y_col: str,
    *,
    title: str = "New High (1Y)",
    color: str = "#f59e0b",
    size: int = 90,
):
    if "IsNewHigh1Y" not in df.columns:
        return None
    if date_col not in df.columns:
        return None
    if y_col not in df.columns:
        return None

    m = df[df["IsNewHigh1Y"] == True].copy()  # noqa: E712 (pandas nullable boolean)
    if m.empty:
        return None

    m = m[[date_col, y_col]].copy().sort_values(date_col)
    m["Event"] = title
    return (
        alt.Chart(m)
        .mark_point(shape="triangle-up", filled=True, size=size, color=color)
        .encode(
            x=alt.X(f"{date_col}:T", title="Date"),
            # Disable axis on marker layer so it doesn't override main axis
            y=alt.Y(f"{y_col}:Q", axis=None),
            tooltip=[
                alt.Tooltip(f"{date_col}:T"),
                alt.Tooltip(f"{y_col}:Q", title=y_col),
                alt.Tooltip("Event:N"),
            ],
        )
    )

def _build_dual_axis_chart(
    df: pd.DataFrame,
    date_col: str,
    left_cols: list[str],
    right_cols: list[str],
    *,
    marker_layer=None,
):
    base = df[[date_col] + sorted(set(left_cols + right_cols))].copy()
    base = base.sort_values(date_col)

    def melt(cols: list[str]) -> pd.DataFrame:
        if not cols:
            return pd.DataFrame(columns=[date_col, "metric", "value"])
        return base[[date_col] + cols].melt(id_vars=[date_col], var_name="metric", value_name="value")

    left_long = melt(left_cols)
    right_long = melt(right_cols)

    x = alt.X(f"{date_col}:T", title="Date")
    left = (
        alt.Chart(left_long)
        .mark_line()
        .encode(
            x=x,
            y=alt.Y("value:Q", title="Left axis"),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=[alt.Tooltip(f"{date_col}:T"), alt.Tooltip("metric:N"), alt.Tooltip("value:Q")],
        )
    )
    if marker_layer is not None:
        # Marker should share the left (price-scale) axis
        left = alt.layer(left, marker_layer)

    if right_cols:
        right = (
            alt.Chart(right_long)
            .mark_line(strokeDash=[6, 2])
            .encode(
                x=x,
                y=alt.Y("value:Q", axis=alt.Axis(orient="right", title="Right axis")),
                color=alt.Color("metric:N", legend=None),
                tooltip=[alt.Tooltip(f"{date_col}:T"), alt.Tooltip("metric:N"), alt.Tooltip("value:Q")],
            )
        )
        return alt.layer(left, right).resolve_scale(y="independent")

    return left

def _build_candlestick_chart(df: pd.DataFrame, date_col: str = "Date", marker_layer=None):
    """
    Candlestick chart from OHLC columns.
    - Rule: Low..High
    - Bar: Open..Close (green up, red down)
    """
    needed = {"Open", "High", "Low", "Close", date_col}
    if not needed.issubset(set(df.columns)):
        return None

    base = df[[date_col, "Open", "High", "Low", "Close"]].copy()
    base = base.sort_values(date_col)
    base["is_up"] = (pd.to_numeric(base["Close"], errors="coerce") >= pd.to_numeric(base["Open"], errors="coerce"))

    x = alt.X(f"{date_col}:T", title="Date")

    wick = (
        alt.Chart(base)
        .mark_rule()
        .encode(
            x=x,
            y=alt.Y("Low:Q", title="Price"),
            y2="High:Q",
            color=alt.condition("datum.is_up", alt.value("#16a34a"), alt.value("#dc2626")),
            tooltip=[
                alt.Tooltip(f"{date_col}:T"),
                alt.Tooltip("Open:Q"),
                alt.Tooltip("High:Q"),
                alt.Tooltip("Low:Q"),
                alt.Tooltip("Close:Q"),
            ],
        )
    )

    body = (
        alt.Chart(base)
        .mark_bar()
        .encode(
            x=x,
            y=alt.Y("Open:Q", title=None),
            y2="Close:Q",
            color=alt.condition("datum.is_up", alt.value("#16a34a"), alt.value("#dc2626")),
            tooltip=[
                alt.Tooltip(f"{date_col}:T"),
                alt.Tooltip("Open:Q"),
                alt.Tooltip("High:Q"),
                alt.Tooltip("Low:Q"),
                alt.Tooltip("Close:Q"),
            ],
        )
    )

    layers = [wick, body]
    if marker_layer is not None:
        layers.append(marker_layer)
    return alt.layer(*layers)

def _build_metric_overlay_lines(df: pd.DataFrame, date_col: str, cols: list[str], axis_orient: str, show_legend: bool):
    if not cols:
        return None
    base = df[[date_col] + cols].copy().sort_values(date_col)
    long = base.melt(id_vars=[date_col], var_name="metric", value_name="value")
    axis = alt.Axis(orient=axis_orient, title=("Right axis" if axis_orient == "right" else "Left axis"))
    return (
        alt.Chart(long)
        .mark_line()
        .encode(
            x=alt.X(f"{date_col}:T", title="Date"),
            y=alt.Y("value:Q", axis=axis),
            color=alt.Color("metric:N", title="Metric", legend=None if not show_legend else alt.Legend()),
            tooltip=[alt.Tooltip(f"{date_col}:T"), alt.Tooltip("metric:N"), alt.Tooltip("value:Q")],
        )
    )

def _build_candlestick_with_metrics(df: pd.DataFrame, date_col: str, metrics: list[str], *, marker_layer=None):
    candle = _build_candlestick_chart(df, date_col, marker_layer=marker_layer)
    if candle is None:
        return None

    metrics = [m for m in metrics if m in df.columns]
    if not metrics:
        return candle

    # Decide left/right for overlays based on Close scale
    # Even though we don't overlay Close as a line, use Close as baseline for scale heuristics.
    left_cols, right_cols = _axis_assignment(df, "Close", metrics)

    # Build left overlay list (optionally includes Close)
    left_overlay = [c for c in left_cols if c in metrics]
    right_overlay = [c for c in right_cols if c in metrics]

    left_lines = _build_metric_overlay_lines(
        df,
        date_col,
        left_overlay,
        axis_orient="left",
        show_legend=True,
    )
    right_lines = _build_metric_overlay_lines(
        df,
        date_col,
        right_overlay,
        axis_orient="right",
        show_legend=(left_lines is None),
    )
    if right_lines is not None:
        # Make right axis dashed for distinction
        right_lines = right_lines.mark_line(strokeDash=[6, 2])

    left_chart = candle if left_lines is None else alt.layer(candle, left_lines)
    if right_lines is None:
        return left_chart

    return alt.layer(left_chart, right_lines).resolve_scale(y="independent")

def find_meta_asset(assets, parquet_asset_name: str):
    """
    parquet 자산과 짝이 되는 meta json을 찾습니다.
    기본 규칙: <name>.parquet -> <name>.meta.json
    """
    expected = parquet_asset_name.replace(".parquet", ".meta.json")
    for a in assets:
        if a.get("name") == expected:
            return a
    return None

def find_asset_by_name(assets, asset_name: str):
    for a in assets:
        if a.get("name") == asset_name:
            return a
    return None

def is_ticker_info_map_asset(asset_name: str) -> bool:
    n = (asset_name or "").lower()
    return (
        n.endswith("_ticker_info_map.parquet")
        or ("ticker_info_map" in n)
        or ("ticker-info-map" in n)
        # backward compat
        or n.endswith("_ticker_name_map.parquet")
        or ("ticker_name_map" in n)
        or ("ticker-name-map" in n)
    )

def pick_meta_asset(assets):
    meta_assets = [a for a in assets if a.get("name", "").endswith(".meta.json")]
    if not meta_assets:
        return None
    # Prefer the known default name if present
    for a in meta_assets:
        if a.get("name") == "korea_universe_feature_frame.meta.json":
            return a
    # Otherwise prefer assets that look like they belong to the feature frame
    for a in meta_assets:
        n = a.get("name", "").lower()
        if "feature" in n and "frame" in n:
            return a
    return meta_assets[0]

def pick_feature_asset(assets):
    parquet_assets = [a for a in assets if a.get("name", "").endswith(".parquet")]
    feature_assets = [a for a in parquet_assets if not is_ticker_info_map_asset(a.get("name", ""))]
    if not feature_assets:
        return None
    # Prefer the known default name if present
    for a in feature_assets:
        if a.get("name") == "korea_universe_feature_frame.parquet":
            return a
    # Otherwise prefer assets that look like they belong to the feature frame
    for a in feature_assets:
        n = a.get("name", "").lower()
        if "feature" in n and "frame" in n:
            return a
    return feature_assets[0]

def pick_ticker_info_map_asset(assets):
    parquet_assets = [a for a in assets if a.get("name", "").endswith(".parquet")]
    map_assets = [a for a in parquet_assets if is_ticker_info_map_asset(a.get("name", ""))]
    if not map_assets:
        return None
    for a in map_assets:
        if a.get("name") == "korea_universe_ticker_info_map.parquet":
            return a
    return map_assets[0]

def pick_krx_stock_master_asset(assets):
    candidates = [a for a in assets if a.get("name", "").endswith(".parquet")]
    if not candidates:
        return None
    for a in candidates:
        if a.get("name") == "krx_stock_master.parquet":
            return a
    for a in candidates:
        if "krx_stock_master" in (a.get("name", "").lower()):
            return a
    return None

# 메인 로직
if repo_name:
    releases = get_releases(repo_name, github_token)

    if releases:
        st.write(f"✅ Found {len(releases)} releases.")
        
        # 릴리스 선택
        release_options = {f"{r['name']} ({r['tag_name']})": r for r in releases}
        selected_option = st.selectbox("Select Release", list(release_options.keys()))
        
        if selected_option:
            selected_release = release_options[selected_option]
            
            with st.expander("Release Details", expanded=True):
                st.markdown(f"**Created at:** {selected_release['created_at']}")
                st.markdown(f"**Tag:** `{selected_release['tag_name']}`")
                st.markdown(selected_release['body'] if selected_release['body'] else "No description.")
            
            # Asset 찾기
            assets = selected_release.get('assets', [])

            st.subheader("📦 Assets")
            meta_asset = pick_meta_asset(assets)
            feature_asset = pick_feature_asset(assets)
            ticker_info_map_asset = pick_ticker_info_map_asset(assets)
            krx_master_asset = pick_krx_stock_master_asset(assets)

            # Keep loaded frames in session_state (so chart UI doesn't reset)
            if "feature_df" not in st.session_state:
                st.session_state["feature_df"] = None
            if "ticker_info_df" not in st.session_state:
                st.session_state["ticker_info_df"] = None
            if "krx_master_df" not in st.session_state:
                st.session_state["krx_master_df"] = None

            # 1) 메타데이터: 릴리즈 선택 시 자동 로드/표시 (meta-only 릴리즈 지원)
            with st.expander("Metadata (meta.json)", expanded=True):
                if meta_asset:
                    st.write(f"**Meta asset:** `{meta_asset['name']}`")
                    meta = load_json_from_url(meta_asset["browser_download_url"], github_token)
                    if meta:
                        col_a, col_b, col_c, col_d = st.columns(4)
                        col_a.metric("Start", meta.get("start_date", "-"))
                        col_b.metric("End", meta.get("end_date", "-"))
                        col_c.metric("Tickers", meta.get("ticker_count", 0))
                        col_d.metric("Rows", meta.get("rows", 0))
                        st.json(meta)
                else:
                    st.info("No meta json found in this release.")

            # 1.5) KRX Stock Master: 버튼 클릭 시 로드
            with st.expander("KRX Stock Master (parquet)", expanded=False):
                if krx_master_asset:
                    st.write(f"**Master asset:** `{krx_master_asset['name']}`")
                    if st.button("Load KRX Stock Master", key="load_krx_master"):
                        with st.spinner("Downloading KRX stock master..."):
                            mdf = load_parquet_from_url(krx_master_asset["browser_download_url"], github_token)
                            if mdf is not None:
                                st.success("KRX stock master loaded successfully!")
                                st.session_state["krx_master_df"] = mdf
                    mdf_loaded = st.session_state.get("krx_master_df")
                    if mdf_loaded is not None:
                        st.write(f"**Loaded shape:** {mdf_loaded.shape}")
                        st.dataframe(mdf_loaded.head(500), use_container_width=True)
                else:
                    st.info("No `krx_stock_master.parquet` found in this release.")

            # 2) 티커 정보 맵: 버튼 클릭 시 로드
            with st.expander("Ticker Info Map (separate parquet)", expanded=True):
                if ticker_info_map_asset:
                    st.write(f"**Info map asset:** `{ticker_info_map_asset['name']}`")
                    if st.button("Load Ticker Info Map", key="load_ticker_info_map"):
                        with st.spinner("Downloading ticker info map..."):
                            tndf = load_parquet_from_url(ticker_info_map_asset["browser_download_url"], github_token)
                            if tndf is not None:
                                st.success("Ticker info map loaded successfully!")
                                st.session_state["ticker_info_df"] = tndf
                    tndf_loaded = st.session_state.get("ticker_info_df")
                    if tndf_loaded is not None:
                        st.write(f"**Loaded shape:** {tndf_loaded.shape}")
                        st.dataframe(tndf_loaded.head(500), use_container_width=True)
                else:
                    st.info("No ticker info map parquet found in this release.")

            # 3) Feature data: 버튼 클릭 시 로드
            with st.expander("Feature Data (parquet)", expanded=True):
                if feature_asset:
                    st.write(f"**Feature asset:** `{feature_asset['name']}`")
                    if st.button("Load Feature Data", key="load_feature_data"):
                        with st.spinner("Downloading and loading feature parquet..."):
                            df = load_parquet_from_url(feature_asset["browser_download_url"], github_token)
                            if df is not None:
                                st.success("Feature data loaded successfully!")
                                st.session_state["feature_df"] = df
                    df_loaded = st.session_state.get("feature_df")
                    if df_loaded is not None:
                        st.write(f"**Loaded shape:** {df_loaded.shape}")
                        st.dataframe(df_loaded.head(200), use_container_width=True)
                else:
                    st.info("No feature parquet found in this release.")

            # 4) Chart: search ticker/name and plot selected series
            df_loaded = st.session_state.get("feature_df")
            if df_loaded is not None and not df_loaded.empty:
                st.subheader("📈 Chart")

                if "Date" not in df_loaded.columns or "Ticker" not in df_loaded.columns:
                    st.error("Feature data must contain `Date` and `Ticker` columns to plot.")
                else:
                    plot_df = df_loaded.copy()
                    plot_df["Date"] = _ensure_datetime(plot_df["Date"])
                    plot_df = plot_df.dropna(subset=["Date"])

                    # Ensure ticker info map is available for name-based search/display
                    info_df = st.session_state.get("ticker_info_df")
                    if (info_df is None or info_df.empty) and ticker_info_map_asset is not None:
                        with st.spinner("Loading ticker info map for search..."):
                            tndf = load_parquet_from_url(ticker_info_map_asset["browser_download_url"], github_token)
                            if tndf is not None and not tndf.empty:
                                st.session_state["ticker_info_df"] = tndf
                                info_df = tndf

                    # Ensure KRX stock master is available (richer market/industry info)
                    master_df = st.session_state.get("krx_master_df")
                    if (master_df is None or master_df.empty) and krx_master_asset is not None:
                        with st.spinner("Loading KRX stock master for market/industry info..."):
                            mdf = load_parquet_from_url(krx_master_asset["browser_download_url"], github_token)
                            if mdf is not None and not mdf.empty:
                                st.session_state["krx_master_df"] = mdf
                                master_df = mdf

                    tickers_in_data = sorted(plot_df["Ticker"].dropna().astype(str).unique().tolist())

                    # Build selectable ticker options
                    options = None
                    if master_df is not None and not master_df.empty and "Code" in master_df.columns:
                        mv = master_df.copy()
                        mv["Code"] = mv["Code"].astype(str)
                        mv = mv[mv["Code"].isin(tickers_in_data)]
                        if "Name" not in mv.columns:
                            mv["Name"] = ""
                        if "Market" not in mv.columns:
                            mv["Market"] = ""
                        # Map to the same schema as selectbox expects
                        mv = mv.rename(columns={"Code": "Ticker"})
                        options = mv.to_dict(orient="records")
                    elif info_df is not None and not info_df.empty and "Ticker" in info_df.columns:
                        info_view = info_df.copy()
                        info_view["Ticker"] = info_view["Ticker"].astype(str)
                        if "Name" not in info_view.columns:
                            info_view["Name"] = ""
                        if "Market" not in info_view.columns:
                            info_view["Market"] = ""
                        info_view = info_view[info_view["Ticker"].isin(tickers_in_data)]
                        options = info_view.to_dict(orient="records")

                    if options is not None:
                        search = st.text_input("Search (Ticker or Name)", value="")
                        if search:
                            s = search.strip().lower()
                            options = [
                                o
                                for o in options
                                if s in str(o.get("Ticker", "")).lower() or s in str(o.get("Name", "")).lower()
                            ]
                        selected = st.selectbox(
                            "Select Ticker",
                            options,
                            format_func=lambda o: f"{o.get('Ticker','')} - {o.get('Name','')} ({o.get('Market','')})",
                        )
                        selected_ticker = str(selected.get("Ticker", ""))
                    else:
                        st.info("Ticker info map not available. (Ticker-only selection)")
                        search = st.text_input("Search (Ticker)", value="")
                        options = tickers_in_data
                        if search:
                            s = search.strip().lower()
                            options = [t for t in options if s in t.lower()]
                        selected_ticker = st.selectbox("Select Ticker", options) if options else ""

                    if not selected_ticker:
                        st.warning("No ticker selected.")
                    else:
                        # Show selected ticker market/industry info (if available)
                        if master_df is not None and not master_df.empty and "Code" in master_df.columns:
                            mv = master_df.copy()
                            mv["Code"] = mv["Code"].astype(str)
                            row = mv[mv["Code"] == selected_ticker]
                            if not row.empty:
                                r0 = row.iloc[0].to_dict()
                                st.markdown(
                                    f"**Selected**: `{selected_ticker}` - {r0.get('Name','')} "
                                    f"(**{r0.get('Market','')}**)\n\n"
                                    f"- **Industry (L/M/S)**: {r0.get('IndustryLarge','')} / {r0.get('IndustryMid','')} / {r0.get('IndustrySmall','')}"
                                )

                        one = plot_df[plot_df["Ticker"].astype(str) == selected_ticker].copy()
                        if one.empty:
                            st.warning("No data for selected ticker.")
                        else:
                            one = one.sort_values("Date")
                            dmin = one["Date"].min()
                            dmax = one["Date"].max()
                            default_start, default_end = _pick_default_date_window(dmin, dmax, days=365)

                            # Streamlit slider wants python datetime/date
                            min_dt = dmin.to_pydatetime()
                            max_dt = dmax.to_pydatetime()
                            default_range = (default_start.to_pydatetime(), default_end.to_pydatetime())
                            date_range = st.slider(
                                "Date range",
                                min_value=min_dt,
                                max_value=max_dt,
                                value=default_range,
                            )
                            start_dt, end_dt = date_range
                            one = one[(one["Date"] >= pd.Timestamp(start_dt)) & (one["Date"] <= pd.Timestamp(end_dt))]

                            if one.empty:
                                st.warning("No data in selected date range.")
                            else:
                                show_newhigh = st.checkbox("Show 1Y New High markers", value=True)
                                marker_pos = st.selectbox("New High marker position", ["High", "Close"], index=0)
                                marker_y_col = marker_pos if marker_pos in one.columns else "Close"
                                newhigh_layer = _build_newhigh_marker_layer(one, "Date", marker_y_col) if show_newhigh else None

                                tab_line, tab_candle = st.tabs(["Close & Metrics (Line)", "Candlestick (OHLC)"])

                                with tab_line:
                                    numeric_cols = one.select_dtypes(include="number").columns.tolist()
                                    if "Close" not in one.columns:
                                        st.error("Selected data does not contain `Close` column.")
                                    else:
                                        extra_candidates = [c for c in numeric_cols if c not in {"Close"}]
                                        extra = st.multiselect(
                                            "Additional numeric metrics (Close is always shown)",
                                            options=extra_candidates,
                                            default=[],
                                        )
                                        metrics = ["Close"] + [c for c in extra if c != "Close"]

                                        # Assign to left/right axes based on scale
                                        left_cols, right_cols = _axis_assignment(one, "Close", [c for c in metrics if c != "Close"])

                                        st.caption(
                                            f"Left axis: {', '.join(left_cols)}"
                                            + (f" | Right axis: {', '.join(right_cols)}" if right_cols else "")
                                        )

                                        chart = _build_dual_axis_chart(one, "Date", left_cols, right_cols, marker_layer=newhigh_layer)
                                        st.altair_chart(chart, use_container_width=True)

                                with tab_candle:
                                    numeric_cols = one.select_dtypes(include="number").columns.tolist()
                                    missing = [c for c in ["Open", "High", "Low", "Close"] if c not in one.columns]
                                    if missing:
                                        st.info("Candlestick requires `Open`, `High`, `Low`, `Close` columns.")
                                    else:
                                        extra_candidates = [
                                            c
                                            for c in numeric_cols
                                            if c not in {"Open", "High", "Low", "Close"}
                                        ]
                                        extra = st.multiselect(
                                            "Additional numeric metrics to overlay",
                                            options=extra_candidates,
                                            default=[],
                                            key="candle_extra_metrics",
                                        )
                                        metrics = [c for c in extra if c != "Close"]
                                        st.caption(
                                            "Overlay metrics are auto-assigned to left/right axis based on scale vs Close."
                                        )
                                        candle = _build_candlestick_with_metrics(one, "Date", metrics, marker_layer=newhigh_layer)
                                        if candle is None:
                                            st.info("Could not build candlestick chart for this data.")
                                        else:
                                            st.altair_chart(candle, use_container_width=True)
    else:
        if repo_name != default_repo:
            st.info("No releases found. Please check the repository name or token.")
else:
    st.info("Please enter a repository name in the sidebar.")
