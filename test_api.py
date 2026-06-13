import json
import urllib.request

def test_api():
    url = 'http://localhost:8050/api/data?limit=5'
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read())
    
    print('=== /api/data 接口测试 ===')
    print()
    print(f"生成时间: {data['generated_at']}")
    print(f"过滤器: {data['filters']}")
    print()
    
    s = data['summary']
    print('--- summary (统计摘要) ---')
    print(f"  数据点数: {s['count']}")
    print(f"  metric_a 均值/最小/最大: {s['metric_a_mean']} / {s['metric_a_min']} / {s['metric_a_max']}")
    print(f"  metric_b 均值: {s['metric_b_mean']}")
    print(f"  metric_c 均值: {s['metric_c_mean']}")
    print(f"  时间范围: {s['time_start'][:19]} ~ {s['time_end'][:19]}")
    print()
    
    ts = data['timeseries']
    print(f'--- timeseries (时间序列, 共{len(ts)}条, limit=5) ---')
    for row in ts[:2]:
        print(f"  {row['timestamp'][:19]}  A={row['metric_a']:>7.2f}  B={row['metric_b']:>7.2f}  C={row['metric_c']:>7.2f}  cat={row['category']}")
    if len(ts) > 2:
        print(f'  ... ({len(ts)-2} more)')
    print()
    
    bc = data['by_category']
    print(f'--- by_category (按类别, 共{len(bc)}类) ---')
    for row in bc:
        print(f"  {row['category']:<12} total_a={row['total_a']:>9.2f}  avg_b={row['avg_b']:>7.2f}  count={row['count']}")
    print()
    
    br = data['by_region']
    print(f'--- by_region (按区域, 共{len(br)}区) ---')
    for row in br:
        print(f"  {row['region']:<6} metric_c_total={row['metric_c_total']:>9.2f}  volume={row['volume']:>7.2f}")
    print()
    
    bs = data['by_status']
    print(f'--- by_status (按状态, 共{len(bs)}种) ---')
    total = sum(x['count'] for x in bs)
    for row in bs:
        pct = row['count']/total*100
        print(f"  {row['status']:<10} {row['count']:>4} ({pct:5.1f}%)")
    print()
    
    print('=== 测试带过滤参数 ===')
    url2 = 'http://localhost:8050/api/data?category=Electronics&limit=3'
    with urllib.request.urlopen(url2) as r:
        d2 = json.loads(r.read())
    print(f"  过滤后数据点: {d2['summary']['count']} (category={d2['filters']['category']})")
    print(f"  所有类别均为 Electronics: {all(r['category']=='Electronics' for r in d2['timeseries'])}")
    
    print()
    print('=== 测试 live=true 新增数据 ===')
    with urllib.request.urlopen('http://localhost:8050/api/data?live=true&limit=1') as r:
        d3 = json.loads(r.read())
    print(f"  新增后最新时间: {d3['timeseries'][0]['timestamp']}")
    
    print()
    print('✅ 所有测试通过!')

if __name__ == '__main__':
    test_api()
