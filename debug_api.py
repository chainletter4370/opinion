"""
Debug script to check Opinion API responses
"""
import os
import json

# Try to load dotenv, but don't fail if not available
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    print("⚠️ dotenv not available, using environment variables directly")

try:
    from opinion_clob_sdk import Client
    from opinion_clob_sdk.model import TopicStatusFilter
    SDK_AVAILABLE = True
except ImportError:
    print("❌ SDK not available")
    SDK_AVAILABLE = False
    exit(1)

def debug_print(label, obj):
    """Pretty print objects"""
    print(f"\n{'='*60}")
    print(f"🔍 {label}")
    print(f"{'='*60}")
    print(f"Type: {type(obj)}")

    # Try to print as dict
    if hasattr(obj, "__dict__"):
        print(json.dumps(obj.__dict__, indent=2, default=str))
    elif hasattr(obj, "model_dump"):
        print(json.dumps(obj.model_dump(), indent=2, default=str))
    else:
        print(str(obj)[:500])
    print()

print("🚀 Starting Opinion API Debug Script")
print(f"API_KEY: {os.getenv('OPINION_API_KEY')[:20]}...")
print(f"PRIVATE_KEY: {'✅ Set' if os.getenv('OPINION_PRIVATE_KEY') else '❌ Missing'}")
print(f"MULTI_SIG_ADDR: {os.getenv('OPINION_MULTI_SIG_ADDR')}")

# Test 1: Initialize Client
print("\n" + "="*60)
print("TEST 1: Client Initialization")
print("="*60)

try:
    # Try with minimal parameters first
    client = Client(
        host='https://proxy.opinion.trade:8443',
        apikey=os.getenv('OPINION_API_KEY'),
        chain_id=56,
        rpc_url=os.getenv('OPINION_RPC_URL', 'https://bsc-dataseed.binance.org'),
        private_key=os.getenv('OPINION_PRIVATE_KEY'),
        multi_sig_addr=os.getenv('OPINION_MULTI_SIG_ADDR')
    )
    print("✅ Client initialized (minimal params)")
except Exception as e:
    print(f"❌ Client init failed (minimal): {e}")
    print("\nTrying with full params...")

    try:
        client = Client(
            host='https://proxy.opinion.trade:8443',
            apikey=os.getenv('OPINION_API_KEY'),
            chain_id=56,
            rpc_url=os.getenv('OPINION_RPC_URL', 'https://bsc-dataseed.binance.org'),
            private_key=os.getenv('OPINION_PRIVATE_KEY'),
            multi_sig_addr=os.getenv('OPINION_MULTI_SIG_ADDR'),
            conditional_tokens_addr='0xAD1a38cEc043e70E83a3eC30443dB285ED10D774',
            multisend_addr='0x998739BFdAAdde7C933B942a68053933098f9EDa'
        )
        print("✅ Client initialized (full params)")
    except Exception as e2:
        print(f"❌ Client init failed (full): {e2}")
        exit(1)

# Test 2: Get Markets
print("\n" + "="*60)
print("TEST 2: get_markets()")
print("="*60)

try:
    response = client.get_markets(page=1, limit=3, status=TopicStatusFilter.ACTIVATED)
    debug_print("get_markets() raw response", response)

    # Check for errno
    if hasattr(response, 'errno'):
        print(f"errno: {response.errno}")
        print(f"errmsg: {response.errmsg}")

        if response.errno == 0:
            result = response.result
            debug_print("response.result", result)

            if hasattr(result, 'list'):
                print(f"\n✅ result.list exists, length: {len(result.list)}")
                if result.list:
                    debug_print("First market in result.list", result.list[0])
            else:
                print("❌ result.list not found")
                print("Available attributes:", dir(result))
    else:
        print("❌ Response has no errno attribute")
        print("Available attributes:", dir(response))

except Exception as e:
    print(f"❌ get_markets() failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Get Market (specific)
print("\n" + "="*60)
print("TEST 3: get_market(2122)")
print("="*60)

try:
    response = client.get_market(2122)
    debug_print("get_market() raw response", response)

    if hasattr(response, 'errno'):
        print(f"errno: {response.errno}")
        if response.errno == 0:
            result = response.result
            debug_print("response.result", result)

            if hasattr(result, 'data'):
                debug_print("response.result.data", result.data)

except Exception as e:
    print(f"❌ get_market() failed: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Get My Positions
print("\n" + "="*60)
print("TEST 4: get_my_positions()")
print("="*60)

try:
    response = client.get_my_positions(limit=3)
    debug_print("get_my_positions() raw response", response)

    if hasattr(response, 'errno'):
        print(f"errno: {response.errno}")
        if response.errno == 0:
            result = response.result
            debug_print("response.result", result)

            # Check different possible structures
            if hasattr(result, 'list'):
                print(f"✅ result.list exists, length: {len(result.list)}")
                if result.list:
                    debug_print("First position", result.list[0])
            elif isinstance(result, list):
                print(f"✅ result is a list, length: {len(result)}")
                if result:
                    debug_print("First position", result[0])
            else:
                print("❌ Unexpected result structure")
                print("Result type:", type(result))

except Exception as e:
    print(f"❌ get_my_positions() failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Get Orderbook
print("\n" + "="*60)
print("TEST 5: get_orderbook() - Need a token_id")
print("="*60)
print("Skipping (need valid token_id from market)")

print("\n" + "="*60)
print("✅ Debug Complete")
print("="*60)
