import json
import threading
import time
import urllib.request
from collections import defaultdict


BASE_URL = 'http://localhost:8050'

call_count = defaultdict(int)
call_lock = threading.Lock()
results = []


def fetch(category, region, status, live=False):
    url = f'{BASE_URL}/api/data?category={category or ""}&region={region or ""}&status={status or ""}'
    if live:
        url += '&live=true'
    start = time.time()
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            source = r.headers.get('X-Data-Source', 'unknown')
            elapsed = (time.time() - start) * 1000
            with call_lock:
                call_count[source] += 1
                results.append({
                    'params': (category, region, status),
                    'source': source,
                    'elapsed_ms': round(elapsed, 2),
                    'count': data['summary']['count'],
                })
            return data, source, elapsed
    except Exception as e:
        elapsed = (time.time() - start) * 1000
        with call_lock:
            call_count['error'] += 1
            results.append({
                'params': (category, region, status),
                'source': 'error',
                'elapsed_ms': round(elapsed, 2),
                'error': str(e),
            })
        return None, 'error', elapsed


def test_rapid_switching():
    print('=' * 70)
    print('  测试 1: 快速切换筛选条件（模拟用户快速切换下拉框）')
    print('=' * 70)

    categories = ['Electronics', 'Clothing', 'Food', 'Home', 'Sports', None,
                  'Electronics', 'Clothing', 'Food', 'Home', 'Sports']

    threads = []
    start_time = time.time()

    for i, cat in enumerate(categories):
        t = threading.Thread(target=fetch, args=(cat, None, None))
        threads.append(t)
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    total_time = (time.time() - start_time) * 1000
    print(f'\n  发送 {len(categories)} 个请求，总耗时: {total_time:.1f}ms')
    print(f'  平均每个请求: {total_time / len(categories):.1f}ms')
    print()
    print('  各来源统计:')
    for source, cnt in sorted(call_count.items()):
        print(f'    {source}: {cnt} 次')
    print()
    print('  详细结果 (前10条):')
    for i, r in enumerate(results[:10]):
        if 'error' in r:
            print(f'    [{i:2d}] params={r["params"]!s:45s} ❌ ERROR: {r["error"][:60]}')
        else:
            print(f'    [{i:2d}] params={r["params"]!s:45s} {r["source"]:>12s}  {r["elapsed_ms"]:7.2f}ms  count={r["count"]}')

    cache_hits = call_count.get('cache', 0)
    sf_hits = call_count.get('singleflight', 0)
    compute = call_count.get('compute', 0)
    total = cache_hits + sf_hits + compute
    saved = 0
    if total > 0:
        saved = (cache_hits + sf_hits) / total * 100
    print()
    print(f'  ✅ 重复请求节省率: {saved:.1f}%  (cache={cache_hits} + singleflight={sf_hits}) / total={total}')

    return saved


def test_same_params_concurrent():
    print()
    print('=' * 70)
    print('  测试 2: 相同参数高并发（模拟多个用户同时请求）')
    print('=' * 70)

    call_count.clear()
    results.clear()

    threads = []
    start_time = time.time()

    for _ in range(20):
        t = threading.Thread(target=fetch, args=('Electronics', 'North', None))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_time = (time.time() - start_time) * 1000
    print(f'\n  发送 20 个相同参数请求，总耗时: {total_time:.1f}ms')
    print()
    print('  各来源统计:')
    for source, cnt in sorted(call_count.items()):
        print(f'    {source}: {cnt} 次')

    compute = call_count.get('compute', 0)
    sf_hits = call_count.get('singleflight', 0)
    total = compute + sf_hits
    saved = 0
    if total > 0:
        saved = sf_hits / total * 100
    print()
    print(f'  ✅ Singleflight 去重率: {saved:.1f}%  (实际仅计算 {compute} 次)')

    return saved


def test_different_params_concurrent():
    print()
    print('=' * 70)
    print('  测试 3: 不同参数高并发（模拟不同用户请求不同数据）')
    print('=' * 70)

    call_count.clear()
    results.clear()

    param_sets = [
        ('Electronics', 'North', 'Active'),
        ('Clothing', 'South', 'Inactive'),
        ('Food', 'East', 'Pending'),
        ('Home', 'West', 'Failed'),
        ('Sports', 'Central', 'Active'),
        ('Electronics', 'South', None),
        ('Clothing', 'North', 'Active'),
        ('Food', 'West', None),
    ]

    threads = []
    start_time = time.time()

    for params in param_sets:
        t = threading.Thread(target=fetch, args=params)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_time = (time.time() - start_time) * 1000
    print(f'\n  发送 {len(param_sets)} 个不同参数请求，总耗时: {total_time:.1f}ms')
    print()
    print('  各来源统计:')
    for source, cnt in sorted(call_count.items()):
        print(f'    {source}: {cnt} 次')

    compute = call_count.get('compute', 0)
    print()
    print(f'  ✅ 不同参数全部独立计算: {compute} 次计算（符合预期）')

    return compute == len(param_sets)


def main():
    print()
    print('🚀 并发请求防抖/去重机制压力测试')
    print()

    try:
        r = urllib.request.urlopen(f'{BASE_URL}/api/health', timeout=3)
        print(f'✅ 服务已启动: {r.read().decode()}')
    except Exception as e:
        print(f'❌ 服务未启动: {e}')
        print(f'   请先运行: python app.py')
        return

    print()
    test1 = test_rapid_switching()
    test2 = test_same_params_concurrent()
    test3 = test_different_params_concurrent()

    print()
    print('=' * 70)
    print('  综合评估')
    print('=' * 70)
    print(f'  测试 1 (快速切换节省率): {test1:.1f}%  {"✅ PASS" if test1 >= 50 else "⚠️  建议优化"}')
    print(f'  测试 2 (相同参数去重率): {test2:.1f}%  {"✅ PASS" if test2 >= 80 else "⚠️  建议优化"}')
    print(f'  测试 3 (不同参数独立):   {"✅ PASS" if test3 else "❌ FAIL"}')
    print()
    if test1 >= 50 and test2 >= 80 and test3:
        print('🎉 全部测试通过！并发防抖/去重机制工作正常。')
    else:
        print('⚠️  部分测试未通过，请检查代码逻辑。')
    print()


if __name__ == '__main__':
    main()
