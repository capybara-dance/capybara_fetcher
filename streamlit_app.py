import streamlit as st
import requests
import pandas as pd
import io
import os

st.set_page_config(page_title="Korea Stock Feature Cache Inspector", layout="wide")

st.title("📊 Korea Stock Feature Cache Inspector")

# 사이드바 설정
st.sidebar.header("Settings")
# 기본값은 현재 사용자 이름/레포 이름 패턴을 가정하거나 비워둡니다.
# 사용자가 직접 입력하도록 안내하는 것이 가장 확실합니다.
default_repo = "cursor-ai/capybara_fetcher" # 예시 값
repo_name = st.sidebar.text_input("Repository (owner/repo)", value=default_repo) 
github_token = st.sidebar.text_input("GitHub Token (Optional, for private repos)", type="password")

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
                selected_asset = st.selectbox(
                    "Select Asset to Load", 
                    parquet_assets, 
                    format_func=lambda x: f"{x['name']} ({x['size']/1024/1024:.2f} MB)"
                )
                
                if st.button("Load Data & Inspect"):
                    with st.spinner("Downloading and loading Parquet file..."):
                        # 주의: browser_download_url은 리다이렉트가 발생하며, Private Repo의 경우 인증 처리가 까다로울 수 있음.
                        # 여기서는 단순 GET 요청으로 시도.
                        df = load_parquet_from_url(selected_asset['browser_download_url'], github_token)
                        
                        if df is not None:
                            st.success("Data loaded successfully!")
                            
                            st.markdown("### 📋 DataFrame Preview")
                            st.write(f"**Shape:** {df.shape}")
                            st.dataframe(df.head(100), use_container_width=True)
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("#### Data Types")
                                st.write(df.dtypes)
                            with col2:
                                st.markdown("#### Descriptive Statistics")
                                st.write(df.describe())
            else:
                st.warning("No .parquet files found in this release.")
    else:
        if repo_name != default_repo:
            st.info("No releases found. Please check the repository name or token.")
else:
    st.info("Please enter a repository name in the sidebar.")
