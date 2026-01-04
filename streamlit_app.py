import streamlit as st
import requests
import pandas as pd
import io
import os
import json

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

def is_ticker_name_map_asset(asset_name: str) -> bool:
    n = (asset_name or "").lower()
    return n.endswith("_ticker_name_map.parquet") or ("ticker_name_map" in n) or ("ticker-name-map" in n)

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
    feature_assets = [a for a in parquet_assets if not is_ticker_name_map_asset(a.get("name", ""))]
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

def pick_ticker_name_map_asset(assets):
    parquet_assets = [a for a in assets if a.get("name", "").endswith(".parquet")]
    map_assets = [a for a in parquet_assets if is_ticker_name_map_asset(a.get("name", ""))]
    if not map_assets:
        return None
    for a in map_assets:
        if a.get("name") == "korea_universe_ticker_name_map.parquet":
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
            parquet_assets = [a for a in assets if a['name'].endswith('.parquet')]
            
            if parquet_assets:
                st.subheader("📦 Assets")
                meta_asset = pick_meta_asset(assets)
                feature_asset = pick_feature_asset(assets)
                ticker_name_map_asset = pick_ticker_name_map_asset(assets)

                # 1) 메타데이터: 릴리즈 선택 시 자동 로드/표시
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

                # 2) 티커-종목명 맵: 버튼 클릭 시 로드
                with st.expander("Ticker-Name Map (separate parquet)", expanded=True):
                    if ticker_name_map_asset:
                        st.write(f"**Map asset:** `{ticker_name_map_asset['name']}`")
                        if st.button("Load Ticker-Name Map", key="load_ticker_name_map"):
                            with st.spinner("Downloading ticker-name map..."):
                                tndf = load_parquet_from_url(ticker_name_map_asset["browser_download_url"], github_token)
                                if tndf is not None:
                                    st.success("Ticker-Name map loaded successfully!")
                                    st.write(f"**Shape:** {tndf.shape}")
                                    st.dataframe(tndf.head(500), use_container_width=True)
                    else:
                        st.info("No ticker-name map parquet found in this release.")

                # 3) Feature data: 버튼 클릭 시 로드
                with st.expander("Feature Data (parquet)", expanded=True):
                    if feature_asset:
                        st.write(f"**Feature asset:** `{feature_asset['name']}`")
                        if st.button("Load Feature Data", key="load_feature_data"):
                            with st.spinner("Downloading and loading feature parquet..."):
                                df = load_parquet_from_url(feature_asset["browser_download_url"], github_token)
                                if df is not None:
                                    st.success("Feature data loaded successfully!")
                                    st.write(f"**Shape:** {df.shape}")
                                    st.dataframe(df.head(200), use_container_width=True)

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.markdown("#### Data Types")
                                        st.write(df.dtypes)
                                    with col2:
                                        st.markdown("#### Descriptive Statistics")
                                        st.write(df.describe())
                    else:
                        st.info("No feature parquet found in this release.")
            else:
                st.warning("No .parquet files found in this release.")
    else:
        if repo_name != default_repo:
            st.info("No releases found. Please check the repository name or token.")
else:
    st.info("Please enter a repository name in the sidebar.")
