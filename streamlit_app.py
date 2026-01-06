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

def _build_dual_axis_chart(df: pd.DataFrame, date_col: str, left_cols: list[str], right_cols: list[str]):
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

def _build_candlestick_chart(df: pd.DataFrame, date_col: str = "Date"):
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

    return alt.layer(wick, body)

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

            # Keep loaded frames in session_state (so chart UI doesn't reset)
            if "feature_df" not in st.session_state:
                st.session_state["feature_df"] = None
            if "ticker_info_df" not in st.session_state:
                st.session_state["ticker_info_df"] = None

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

                    info_df = st.session_state.get("ticker_info_df")
                    tickers_in_data = sorted(plot_df["Ticker"].dropna().astype(str).unique().tolist())

                    # Build selectable ticker options
                    if info_df is not None and not info_df.empty and "Ticker" in info_df.columns:
                        info_view = info_df.copy()
                        info_view["Ticker"] = info_view["Ticker"].astype(str)
                        if "Name" not in info_view.columns:
                            info_view["Name"] = ""
                        if "Market" not in info_view.columns:
                            info_view["Market"] = ""
                        info_view = info_view[info_view["Ticker"].isin(tickers_in_data)]
                        options = info_view.to_dict(orient="records")
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
                        st.info("Load `Ticker Info Map` to enable name-based search. (Ticker-only selection available.)")
                        search = st.text_input("Search (Ticker)", value="")
                        options = tickers_in_data
                        if search:
                            s = search.strip().lower()
                            options = [t for t in options if s in t.lower()]
                        selected_ticker = st.selectbox("Select Ticker", options) if options else ""

                    if not selected_ticker:
                        st.warning("No ticker selected.")
                    else:
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

                                        chart = _build_dual_axis_chart(one, "Date", left_cols, right_cols)
                                        st.altair_chart(chart, use_container_width=True)

                                with tab_candle:
                                    candle = _build_candlestick_chart(one, "Date")
                                    if candle is None:
                                        st.info("Candlestick requires `Open`, `High`, `Low`, `Close` columns.")
                                    else:
                                        st.altair_chart(candle, use_container_width=True)
    else:
        if repo_name != default_repo:
            st.info("No releases found. Please check the repository name or token.")
else:
    st.info("Please enter a repository name in the sidebar.")
