"""
Example usage of CompositeProvider (skeleton demonstration).

This example shows how to instantiate and use the CompositeProvider.
External users don't need to know about individual provider implementations.
Note: The actual implementation body is pending discussion.
"""

from capybara_fetcher.providers import CompositeProvider

# Example 1: Simple usage with default providers (pykrx)
print("Example 1: Default provider (pykrx)")
composite = CompositeProvider(
    master_json_path="data/krx_stock_master.json"
)
print(f"Created composite provider: {composite.name}")
print(f"Number of providers: {len(composite.providers)}")
print(f"Provider types: {[p.name for p in composite.providers]}")

# Example 2: Multiple providers (pykrx + fdr)
print("\n" + "="*60)
print("Example 2: Multiple providers (pykrx + fdr)")
composite_multi = CompositeProvider(
    master_json_path="data/krx_stock_master.json",
    provider_types=["pykrx", "fdr"],
    fdr_source="KRX"
)
print(f"Created composite provider: {composite_multi.name}")
print(f"Number of providers: {len(composite_multi.providers)}")
print(f"Provider types: {[p.name for p in composite_multi.providers]}")

# Example 3: With custom name
print("\n" + "="*60)
print("Example 3: Custom name")
composite_custom = CompositeProvider(
    master_json_path="data/krx_stock_master.json",
    provider_types=["fdr"],
    fdr_source="NAVER",
    name="my_custom_composite"
)
print(f"Created composite provider: {composite_custom.name}")

# Note: Actual data fetching methods will raise NotImplementedError
# until implementation strategy is determined through discussion
print("\n" + "="*60)
print("Testing interface methods (will raise NotImplementedError):")

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
print("✓ External users don't need to know about individual provider types!")
