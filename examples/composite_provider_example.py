"""
Example usage of CompositeProvider (skeleton demonstration).

This example shows how to instantiate and use the CompositeProvider.
Note: The actual implementation body is pending discussion.
"""

from capybara_fetcher.providers import CompositeProvider, PykrxProvider, FdrProvider

# Example: Create individual providers
pykrx_provider = PykrxProvider(
    master_json_path="data/krx_stock_master.json"
)

fdr_provider = FdrProvider(
    master_json_path="data/krx_stock_master.json",
    source="KRX"
)

# Create a composite provider that combines multiple providers
composite = CompositeProvider(
    providers=[pykrx_provider, fdr_provider],
    name="my_composite"
)

print(f"Created composite provider: {composite.name}")
print(f"Number of providers: {len(composite.providers)}")
print(f"Provider types: {[p.name for p in composite.providers]}")

# Note: Actual data fetching methods will raise NotImplementedError
# until implementation strategy is determined through discussion
try:
    tickers, markets = composite.list_tickers()
except NotImplementedError as e:
    print(f"\nlist_tickers() - {str(e)[:80]}...")

try:
    master = composite.load_stock_master()
except NotImplementedError as e:
    print(f"\nload_stock_master() - {str(e)[:80]}...")

try:
    df = composite.fetch_ohlcv(ticker="005930", start_date="2024-01-01", end_date="2024-01-31")
except NotImplementedError as e:
    print(f"\nfetch_ohlcv() - {str(e)[:80]}...")

print("\n✓ CompositeProvider interface is ready for implementation!")
