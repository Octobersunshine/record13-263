import datetime
import hashlib
import json
import random
import threading
import time
import uuid

import numpy as np
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, callback, ctx, no_update, clientside_callback, ClientsideFunction
from dash.exceptions import PreventUpdate
from flask import Flask, jsonify, request, render_template_string
from flask_sock import Sock
from plotly import graph_objs as go

server = Flask(__name__)
sock = Sock(server)
app = Dash(__name__, server=server, url_base_pathname='/dashboard/',
           suppress_callback_exceptions=True,
           )
app.config.suppress_callback_exceptions = True


@server.after_request
def add_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

_lock = threading.Lock()
_data_cache = {
    'generated_at': None,
    'timestamp': [],
    'metric_a': [],
    'metric_b': [],
    'metric_c': [],
    'category': [],
    'region': [],
    'status': [],
}

_api_cache = {}
_api_cache_lock = threading.Lock()
_api_cache_ttl = 2.0

_singleflight = {}
_singleflight_lock = threading.Lock()

_ws_clients = {}
_ws_lock = threading.Lock()
_ws_push_seq = 0
_ws_push_lock = threading.Lock()
_broadcast_event = threading.Event()
_live_append_interval = 3.0


def _ws_next_push_seq():
    global _ws_push_seq
    with _ws_push_lock:
        _ws_push_seq += 1
        return _ws_push_seq


def _ws_register(ws):
    sid = uuid.uuid4().hex
    with _ws_lock:
        _ws_clients[sid] = {
            'ws': ws,
            'filters': None,
            'metrics': ['metric_a', 'metric_b', 'metric_c'],
            'alive': True,
        }
    return sid


def _ws_unregister(sid):
    with _ws_lock:
        client = _ws_clients.pop(sid, None)
        if client:
            client['alive'] = False


def _ws_update_filters(sid, filters_dict, metrics):
    with _ws_lock:
        client = _ws_clients.get(sid)
        if client:
            client['filters'] = filters_dict
            client['metrics'] = list(metrics) if metrics else ['metric_a', 'metric_b', 'metric_c']


def _build_filtered_snapshot(filters_dict):
    if filters_dict is None:
        filters_dict = {}
    params = {
        'category': filters_dict.get('category'),
        'region': filters_dict.get('region'),
        'status': filters_dict.get('status'),
        'limit': None,
        'live': False,
        'refresh': False,
    }
    try:
        data, _ = _get_data_with_singleflight(params)
        return data
    except Exception:
        return None


def _ws_send_one(sid, client, message):
    if not client.get('alive'):
        return False
    try:
        payload = json.dumps(message, ensure_ascii=False, default=str)
        client['ws'].send(payload)
        return True
    except Exception:
        client['alive'] = False
        with _ws_lock:
            if _ws_clients.get(sid) is client:
                _ws_clients.pop(sid, None)
        return False


def _broadcast_push(reason='timer'):
    seq = _ws_next_push_seq()
    with _ws_lock:
        snapshot = list(_ws_clients.items())

    if not snapshot:
        return

    def _worker():
        for sid, client in snapshot:
            filters_dict = client.get('filters')
            data = _build_filtered_snapshot(filters_dict)
            if data is None:
                continue
            msg = {
                'type': 'data_updated',
                'seq': seq,
                'time': _iso(datetime.datetime.now()),
                'reason': reason,
                'data': data,
            }
            _ws_send_one(sid, client, msg)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _append_and_broadcast():
    _append_new_point()
    _broadcast_push(reason='new_data')


_live_thread_running = False
_live_thread_lock = threading.Lock()


def _start_live_thread():
    global _live_thread_running
    with _live_thread_lock:
        if _live_thread_running:
            return
        _live_thread_running = True

    def _loop():
        global _live_thread_running
        try:
            while True:
                time.sleep(_live_append_interval)
                try:
                    _append_and_broadcast()
                except Exception:
                    pass
        finally:
            with _live_thread_lock:
                _live_thread_running = False

    t = threading.Thread(target=_loop, daemon=True, name='ws-live-push')
    t.start()

MAX_POINTS = 200
CATEGORIES = ['Electronics', 'Clothing', 'Food', 'Home', 'Sports']
REGIONS = ['North', 'South', 'East', 'West', 'Central']
STATUSES = ['Active', 'Inactive', 'Pending', 'Failed']
SEED = 42


def _init_seed():
    random.seed(SEED)
    np.random.seed(SEED)


def _generate_row(base_time, index):
    t = base_time + datetime.timedelta(seconds=index)
    trend = index * 0.15
    a = round(50 + trend + np.sin(index / 8) * 12 + np.random.normal(0, 3), 2)
    b = round(120 + trend * 0.5 + np.cos(index / 12) * 20 + np.random.normal(0, 5), 2)
    c = round(200 - trend * 0.3 + np.sin(index / 5) * 30 + np.random.normal(0, 8), 2)
    cat = random.choice(CATEGORIES)
    reg = random.choice(REGIONS)
    sts = random.choices(STATUSES, weights=[55, 20, 15, 10])[0]
    return t, a, b, c, cat, reg, sts


def _ensure_data():
    with _lock:
        if _data_cache['generated_at'] is not None:
            return _data_cache
        _init_seed()
        base = datetime.datetime.now() - datetime.timedelta(seconds=MAX_POINTS)
        rows = [_generate_row(base, i) for i in range(MAX_POINTS)]
        ts, ma, mb, mc, cats, regs, stss = zip(*rows)
        _data_cache['timestamp'] = list(ts)
        _data_cache['metric_a'] = list(ma)
        _data_cache['metric_b'] = list(mb)
        _data_cache['metric_c'] = list(mc)
        _data_cache['category'] = list(cats)
        _data_cache['region'] = list(regs)
        _data_cache['status'] = list(stss)
        _data_cache['generated_at'] = datetime.datetime.now()
        return _data_cache


def _append_new_point():
    with _lock:
        cache = _data_cache
        if not cache['timestamp']:
            return
        last_ts = cache['timestamp'][-1]
        last_idx = len(cache['timestamp'])
        new_ts, a, b, c, cat, reg, sts = _generate_row(last_ts, last_idx)
        for key, val in [
            ('timestamp', new_ts), ('metric_a', a), ('metric_b', b), ('metric_c', c),
            ('category', cat), ('region', reg), ('status', sts),
        ]:
            cache[key].append(val)
            if len(cache[key]) > MAX_POINTS:
                cache[key].pop(0)
        cache['generated_at'] = datetime.datetime.now()


def build_dataframe():
    cache = _ensure_data()
    with _lock:
        df = pd.DataFrame({
            'timestamp': cache['timestamp'],
            'metric_a': cache['metric_a'],
            'metric_b': cache['metric_b'],
            'metric_c': cache['metric_c'],
            'category': cache['category'],
            'region': cache['region'],
            'status': cache['status'],
        })
    return df


def _iso(dt):
    if isinstance(dt, (pd.Timestamp, datetime.datetime)):
        return dt.isoformat()
    return str(dt)


def aggregate_by_category(df):
    grp = df.groupby('category').agg(
        total_a=('metric_a', 'sum'),
        avg_b=('metric_b', 'mean'),
        count=('category', 'size'),
    ).reset_index()
    grp['avg_b'] = grp['avg_b'].round(2)
    grp['total_a'] = grp['total_a'].round(2)
    return grp.to_dict(orient='records')


def aggregate_by_region(df):
    grp = df.groupby('region').agg(
        metric_c_total=('metric_c', 'sum'),
        metric_c_avg=('metric_c', 'mean'),
        volume=('metric_a', 'mean'),
    ).reset_index()
    for col in ['metric_c_total', 'metric_c_avg', 'volume']:
        grp[col] = grp[col].round(2)
    return grp.to_dict(orient='records')


def aggregate_by_status(df):
    grp = df.groupby('status').size().reset_index(name='count')
    return grp.to_dict(orient='records')


def summary_stats(df):
    return {
        'count': int(len(df)),
        'metric_a_mean': round(float(df['metric_a'].mean()), 2),
        'metric_a_min': round(float(df['metric_a'].min()), 2),
        'metric_a_max': round(float(df['metric_a'].max()), 2),
        'metric_b_mean': round(float(df['metric_b'].mean()), 2),
        'metric_b_min': round(float(df['metric_b'].min()), 2),
        'metric_b_max': round(float(df['metric_b'].max()), 2),
        'metric_c_mean': round(float(df['metric_c'].mean()), 2),
        'metric_c_min': round(float(df['metric_c'].min()), 2),
        'metric_c_max': round(float(df['metric_c'].max()), 2),
        'time_start': _iso(df['timestamp'].min()),
        'time_end': _iso(df['timestamp'].max()),
    }


def timeseries_data(df, limit=None):
    if limit and limit > 0:
        df = df.tail(int(limit))
    recs = []
    for _, row in df.iterrows():
        recs.append({
            'timestamp': _iso(row['timestamp']),
            'metric_a': float(row['metric_a']),
            'metric_b': float(row['metric_b']),
            'metric_c': float(row['metric_c']),
            'category': row['category'],
            'region': row['region'],
            'status': row['status'],
        })
    return recs


def _params_key(category, region, status, limit, live, refresh):
    _ensure_data()
    data_ver = _data_cache.get('generated_at', 0)
    raw = f"cat={category}|reg={region}|sts={status}|lim={limit}|live={live}|ref={refresh}|data_ver={data_ver}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def _get_cached(key):
    with _api_cache_lock:
        entry = _api_cache.get(key)
        if entry and (time.time() - entry['time']) < _api_cache_ttl:
            return entry['data']
    return None


def _set_cached(key, data):
    with _api_cache_lock:
        _api_cache[key] = {'time': time.time(), 'data': data}


def _compute_data(params):
    category, region, status, limit, live, refresh = (
        params['category'], params['region'], params['status'],
        params['limit'], params['live'], params['refresh'],
    )
    if refresh:
        with _lock:
            for key in list(_data_cache.keys()):
                if key != 'generated_at':
                    _data_cache[key] = []
            _data_cache['generated_at'] = None
        with _api_cache_lock:
            _api_cache.clear()
    if live:
        _append_new_point()
    df = build_dataframe()

    if category:
        df = df[df['category'] == category]
    if region:
        df = df[df['region'] == region]
    if status:
        df = df[df['status'] == status]

    response = {
        'generated_at': _iso(datetime.datetime.now()),
        'filters': {
            'category': category,
            'region': region,
            'status': status,
            'limit': limit,
        },
        'summary': summary_stats(df),
        'timeseries': timeseries_data(df, limit),
        'by_category': aggregate_by_category(df),
        'by_region': aggregate_by_region(df),
        'by_status': aggregate_by_status(df),
    }
    return response


def _get_data_with_singleflight(params):
    category, region, status, limit, live, refresh = (
        params['category'], params['region'], params['status'],
        params['limit'], params['live'], params['refresh'],
    )
    key = _params_key(category, region, status, limit, live, refresh)

    if not live and not refresh:
        cached = _get_cached(key)
        if cached is not None:
            return cached, 'cache'

    with _singleflight_lock:
        if key in _singleflight:
            event, result_holder = _singleflight[key]
            is_primary = False
        else:
            event = threading.Event()
            result_holder = {}
            _singleflight[key] = (event, result_holder)
            is_primary = True

    if is_primary:
        try:
            data = _compute_data(params)
            result_holder['data'] = data
            result_holder['source'] = 'compute'
            if not live and not refresh:
                _set_cached(key, data)
            return data, 'compute'
        except Exception as e:
            result_holder['error'] = str(e)
            raise
        finally:
            event.set()
            with _singleflight_lock:
                _singleflight.pop(key, None)
    else:
        event.wait()
        if 'error' in result_holder:
            raise RuntimeError(result_holder['error'])
        return result_holder['data'], 'singleflight'


@server.route('/api/data', methods=['GET'])
def api_data():
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    live = request.args.get('live', 'false').lower() == 'true'
    category = request.args.get('category')
    region = request.args.get('region')
    status = request.args.get('status')
    limit_raw = request.args.get('limit')
    limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None

    params = {
        'category': category,
        'region': region,
        'status': status,
        'limit': limit,
        'live': live,
        'refresh': force_refresh,
    }

    data, source = _get_data_with_singleflight(params)
    resp = jsonify(data)
    resp.headers['X-Data-Source'] = source
    return resp


@server.route('/api/health', methods=['GET'])
def api_health():
    with _ws_lock:
        ws_count = len(_ws_clients)
    return jsonify({
        'status': 'ok',
        'time': _iso(datetime.datetime.now()),
        'websocket_clients': ws_count,
        'data_points': len(_data_cache.get('timestamp', [])),
    })


@sock.route('/ws')
def ws_endpoint(ws):
    sid = _ws_register(ws)
    try:
        hello = {
            'type': 'hello',
            'sid': sid,
            'time': _iso(datetime.datetime.now()),
            'append_interval_sec': _live_append_interval,
        }
        _ws_send_one(sid, _ws_clients.get(sid), hello)

        initial_data = _build_filtered_snapshot({})
        if initial_data:
            init_msg = {
                'type': 'data_updated',
                'seq': _ws_next_push_seq(),
                'time': _iso(datetime.datetime.now()),
                'reason': 'connect',
                'data': initial_data,
            }
            _ws_send_one(sid, _ws_clients.get(sid), init_msg)

        while True:
            try:
                raw = ws.receive(timeout=30)
            except Exception:
                break
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            mtype = msg.get('type')
            if mtype == 'subscribe':
                filters = msg.get('filters') or {}
                metrics = msg.get('metrics') or ['metric_a', 'metric_b', 'metric_c']
                cat = filters.get('category') or None
                reg = filters.get('region') or None
                sts = filters.get('status') or None
                _ws_update_filters(sid, {
                    'category': cat,
                    'region': reg,
                    'status': sts,
                }, metrics)
                ack_data = _build_filtered_snapshot({
                    'category': cat, 'region': reg, 'status': sts,
                })
                if ack_data:
                    ack = {
                        'type': 'data_updated',
                        'seq': _ws_next_push_seq(),
                        'time': _iso(datetime.datetime.now()),
                        'reason': 'subscribe',
                        'data': ack_data,
                    }
                    _ws_send_one(sid, _ws_clients.get(sid), ack)
            elif mtype == 'ping':
                pong = {
                    'type': 'pong',
                    'time': _iso(datetime.datetime.now()),
                    'echo': msg.get('echo'),
                }
                _ws_send_one(sid, _ws_clients.get(sid), pong)
            elif mtype == 'refresh':
                with _lock:
                    for key in list(_data_cache.keys()):
                        if key != 'generated_at':
                            _data_cache[key] = []
                    _data_cache['generated_at'] = None
                with _api_cache_lock:
                    _api_cache.clear()
                _ensure_data()
                _broadcast_push(reason='refresh')
    finally:
        _ws_unregister(sid)


_debounce_delay_ms = 400
_request_seq = 0
_request_seq_lock = threading.Lock()
_in_flight_seq = 0
_in_flight_lock = threading.Lock()
_latest_processed_seq = 0
_latest_processed_lock = threading.Lock()


def _next_seq():
    global _request_seq
    with _request_seq_lock:
        _request_seq += 1
        return _request_seq


def _set_in_flight(seq):
    global _in_flight_seq
    with _in_flight_lock:
        if seq > _in_flight_seq:
            _in_flight_seq = seq
            return True
        return False


def _is_in_flight_valid(seq):
    with _in_flight_lock:
        return seq >= _in_flight_seq


def _set_latest_processed(seq):
    global _latest_processed_seq
    with _latest_processed_lock:
        if seq > _latest_processed_seq:
            _latest_processed_seq = seq
            return True
        return False


def _get_latest_processed():
    with _latest_processed_lock:
        return _latest_processed_seq


def _normalize_filters(metrics, category, region, status):
    cat = category if category else None
    reg = region if region else None
    sts = status if status else None
    return {
        'metrics': list(metrics) if metrics else ['metric_a', 'metric_b', 'metric_c'],
        'category': cat,
        'region': reg,
        'status': sts,
    }


def _filters_equal(a, b):
    if a is None or b is None:
        return a == b
    return (sorted(a.get('metrics', [])) == sorted(b.get('metrics', []))
            and a.get('category') == b.get('category')
            and a.get('region') == b.get('region')
            and a.get('status') == b.get('status'))


def _initial_filters():
    return _normalize_filters(
        ['metric_a', 'metric_b', 'metric_c'], None, None, None)


@app.callback(
    Output('pending-filters', 'data'),
    Input('metric-select', 'value'),
    Input('category-select', 'value'),
    Input('region-select', 'value'),
    Input('status-select', 'value'),
    prevent_initial_call=True,
)
def on_filter_change(metrics, category, region, status):
    current = _normalize_filters(metrics, category, region, status)
    return {'filters': current, 'time': time.time() * 1000}


@app.callback(
    Output('debounced-filters', 'data'),
    Output('filter-trigger', 'data'),
    Input('debounce-interval', 'n_intervals'),
    State('pending-filters', 'data'),
    State('debounced-filters', 'data'),
    State('filter-trigger', 'data'),
    prevent_initial_call=False,
)
def apply_debounced_filters(n, pending, debounced, trigger):
    triggered_by = ctx.triggered_id if hasattr(ctx, 'triggered_id') else None

    if debounced is None:
        initial = _initial_filters()
        new_trigger = {'seq': _next_seq(), 'time': time.time() * 1000}
        return initial, new_trigger

    if pending is None:
        raise PreventUpdate

    now = time.time() * 1000
    elapsed = now - pending['time']
    if elapsed < _debounce_delay_ms:
        raise PreventUpdate

    pending_filters = pending['filters']

    if _filters_equal(pending_filters, debounced):
        raise PreventUpdate

    new_trigger = {'seq': _next_seq(), 'time': now}
    return pending_filters, new_trigger


@app.callback(
    Output('kpi-count', 'children'),
    Output('kpi-a-mean', 'children'),
    Output('kpi-b-mean', 'children'),
    Output('kpi-c-mean', 'children'),
    Output('timeseries-fig', 'figure'),
    Output('category-fig', 'figure'),
    Output('region-fig', 'figure'),
    Output('status-fig', 'figure'),
    Output('last-update', 'children'),
    Input('interval-component', 'n_intervals'),
    Input('filter-trigger', 'data'),
    State('debounced-filters', 'data'),
    State('live-switch', 'on'),
    prevent_initial_call=False,
)
def update_dashboard(n, trigger, filters, live):
    triggered_by = ctx.triggered_id if hasattr(ctx, 'triggered_id') else None

    filters = filters if filters is not None else _initial_filters()
    metrics = filters.get('metrics', ['metric_a', 'metric_b', 'metric_c'])
    category = filters.get('category')
    region = filters.get('region')
    status = filters.get('status')

    current_seq = trigger.get('seq', 0) if trigger else 0
    latest_processed = _get_latest_processed()

    if triggered_by is None:
        if current_seq == 0 or latest_processed == 0:
            current_seq = _next_seq()
            if not _set_in_flight(current_seq):
                raise PreventUpdate
        else:
            raise PreventUpdate
    elif triggered_by == 'filter-trigger':
        if current_seq <= latest_processed:
            raise PreventUpdate
        if not _set_in_flight(current_seq):
            raise PreventUpdate
    elif triggered_by == 'interval-component':
        next_seq = _next_seq()
        if not _set_in_flight(next_seq):
            raise PreventUpdate
        current_seq = next_seq

    params = {
        'category': category,
        'region': region,
        'status': status,
        'limit': None,
        'live': bool(live) and triggered_by == 'interval-component',
        'refresh': False,
    }

    try:
        data, source = _get_data_with_singleflight(params)
    except Exception as e:
        return (no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update,
                f"❌ 数据加载失败: {str(e)}")

    if not _is_in_flight_valid(current_seq):
        raise PreventUpdate

    if not _set_latest_processed(current_seq):
        raise PreventUpdate

    stats = data['summary']
    kpi_count = f"{stats['count']:,}"
    kpi_a = f"{stats['metric_a_mean']:.2f}"
    kpi_b = f"{stats['metric_b_mean']:.2f}"
    kpi_c = f"{stats['metric_c_mean']:.2f}"
    last_update = f"最后更新: {data['generated_at']} ({source}, seq={current_seq})"

    metric_cols = metrics or ['metric_a', 'metric_b', 'metric_c']
    ts_fig = go.Figure()
    palette = {'metric_a': '#636EFA', 'metric_b': '#EF553B', 'metric_c': '#00CC96'}
    labels = {'metric_a': '指标 A (响应)', 'metric_b': '指标 B (吞吐)', 'metric_c': '指标 C (负载)'}

    ts_df = pd.DataFrame(data['timeseries'])
    if not ts_df.empty:
        ts_df['timestamp'] = pd.to_datetime(ts_df['timestamp'])
        for col in metric_cols:
            if col in ts_df.columns:
                ts_fig.add_trace(go.Scatter(
                    x=ts_df['timestamp'], y=ts_df[col], mode='lines', name=labels.get(col, col),
                    line=dict(color=palette.get(col, None), width=2),
                ))
    ts_fig.update_layout(
        title='指标时间序列',
        xaxis_title='时间', yaxis_title='数值',
        template='plotly_white', margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )

    cat_df = pd.DataFrame(data['by_category'])
    cat_fig = go.Figure()
    if not cat_df.empty:
        cat_fig.add_trace(go.Bar(x=cat_df['category'], y=cat_df['total_a'], name='指标 A 合计',
                                 marker_color='#636EFA'))
        cat_fig.add_trace(go.Bar(x=cat_df['category'], y=cat_df['avg_b'], name='指标 B 均值',
                                 marker_color='#EF553B', yaxis='y2'))
    cat_fig.update_layout(
        title='按类别统计',
        xaxis_title='类别',
        yaxis=dict(title='指标 A 合计'),
        yaxis2=dict(title='指标 B 均值', overlaying='y', side='right'),
        template='plotly_white', barmode='group', margin=dict(l=40, r=40, t=50, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )

    reg_df = pd.DataFrame(data['by_region'])
    reg_fig = go.Figure()
    if not reg_df.empty:
        reg_fig.add_trace(go.Scatter(
            x=reg_df['volume'], y=reg_df['metric_c_avg'],
            mode='markers+text', text=reg_df['region'], textposition='top center',
            marker=dict(size=reg_df['metric_c_total'] / 80, color='#00CC96',
                        line=dict(width=1, color='DarkSlateGrey')),
            name='区域分布', showlegend=False,
        ))
    reg_fig.update_layout(
        title='区域散点 (气泡大小=指标C总量)',
        xaxis_title='平均指标 A (流量)', yaxis_title='平均指标 C (负载)',
        template='plotly_white', margin=dict(l=40, r=20, t=50, b=40),
    )

    sts_df = pd.DataFrame(data['by_status'])
    status_colors = {'Active': '#00CC96', 'Inactive': '#AB63FA',
                     'Pending': '#FFA15A', 'Failed': '#EF553B'}
    sts_fig = go.Figure()
    if not sts_df.empty:
        sts_fig.add_trace(go.Pie(
            labels=sts_df['status'], values=sts_df['count'],
            marker=dict(colors=[status_colors.get(s, '#888') for s in sts_df['status']]),
            hole=0.4,
        ))
    sts_fig.update_layout(
        title='状态分布', template='plotly_white',
        margin=dict(l=20, r=20, t=50, b=20),
    )

    return (kpi_count, kpi_a, kpi_b, kpi_c,
            ts_fig, cat_fig, reg_fig, sts_fig, last_update)


app.layout = html.Div([
    dcc.Store(id='pending-filters', storage_type='memory'),
    dcc.Store(id='debounced-filters', storage_type='memory'),
    dcc.Store(id='filter-trigger', storage_type='memory', data={'seq': 0, 'time': 0}),
    dcc.Store(id='ws-payload', storage_type='memory', data=None),
    dcc.Store(id='ws-last-seq', storage_type='memory', data=0),
    dcc.Interval(id='debounce-interval', interval=100, n_intervals=0),
    dcc.Interval(id='interval-component', interval=300000, n_intervals=0),
    html.Header(children=[
        html.Div([
            html.H1('动态数据仪表盘', style={'margin': 0, 'fontSize': '24px'}),
            html.Div(children=[
                html.Span(id='ws-status', children='●',
                          style={'display': 'inline-block', 'width': '10px', 'height': '10px',
                                 'borderRadius': '50%', 'background': '#bbb',
                                 'marginRight': '6px'}),
                html.Span(id='ws-status-text', children='WebSocket 连接中...',
                          style={'fontSize': '12px', 'color': '#888'}),
            ], style={'marginTop': '6px', 'display': 'flex', 'alignItems': 'center'}),
        ], style={'display': 'flex', 'flexDirection': 'column'}),
        html.Div(id='last-update', style={'marginTop': '6px', 'fontSize': '13px', 'color': '#666',
                                           'textAlign': 'right', 'minWidth': '280px'}),
    ], style={'padding': '16px 24px', 'borderBottom': '1px solid #eee',
              'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'flex-end'}),

    html.Div(children=[
        html.Div([
            html.Label('展示指标: ', style={'marginRight': '8px', 'fontSize': '14px'}),
            dcc.Checklist(
                id='metric-select',
                options=[
                    {'label': ' 指标 A (响应)', 'value': 'metric_a'},
                    {'label': ' 指标 B (吞吐)', 'value': 'metric_b'},
                    {'label': ' 指标 C (负载)', 'value': 'metric_c'},
                ],
                value=['metric_a', 'metric_b', 'metric_c'],
                inline=True,
                labelStyle={'marginRight': '16px', 'fontSize': '14px'},
            ),
        ], style={'flex': 2}),
        html.Div([
            html.Label('类别: ', style={'marginRight': '6px', 'fontSize': '13px', 'color': '#555'}),
            dcc.Dropdown(
                id='category-select',
                options=[{'label': '全部', 'value': ''}] + [{'label': c, 'value': c} for c in CATEGORIES],
                value='',
                clearable=False,
                searchable=False,
                style={'width': '130px', 'display': 'inline-block', 'fontSize': '13px'},
            ),
        ], style={'flex': 1, 'display': 'flex', 'alignItems': 'center'}),
        html.Div([
            html.Label('区域: ', style={'marginRight': '6px', 'fontSize': '13px', 'color': '#555'}),
            dcc.Dropdown(
                id='region-select',
                options=[{'label': '全部', 'value': ''}] + [{'label': r, 'value': r} for r in REGIONS],
                value='',
                clearable=False,
                searchable=False,
                style={'width': '110px', 'display': 'inline-block', 'fontSize': '13px'},
            ),
        ], style={'flex': 1, 'display': 'flex', 'alignItems': 'center'}),
        html.Div([
            html.Label('状态: ', style={'marginRight': '6px', 'fontSize': '13px', 'color': '#555'}),
            dcc.Dropdown(
                id='status-select',
                options=[{'label': '全部', 'value': ''}] + [{'label': s, 'value': s} for s in STATUSES],
                value='',
                clearable=False,
                searchable=False,
                style={'width': '110px', 'display': 'inline-block', 'fontSize': '13px'},
            ),
        ], style={'flex': 1, 'display': 'flex', 'alignItems': 'center'}),
        html.Div([
            dcc.Checklist(
                id='live-switch',
                options=[{'label': ' 实时推送数据', 'value': 'on'}],
                value=['on'],
                labelStyle={'fontSize': '14px'},
            ),
        ], style={'flex': 1, 'textAlign': 'right'}),
    ], style={'padding': '12px 24px', 'display': 'flex', 'alignItems': 'center', 'gap': '12px',
              'borderBottom': '1px solid #f3f3f3', 'background': '#fafafa'}),

    html.Div(children=[
        html.Div(children=[
            html.Div('数据点数', style={'fontSize': '12px', 'color': '#888'}),
            html.Div(id='kpi-count', style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#636EFA'}),
        ], style={'padding': '16px', 'background': '#fff', 'borderRadius': '8px',
                  'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
        html.Div(children=[
            html.Div('指标 A 均值', style={'fontSize': '12px', 'color': '#888'}),
            html.Div(id='kpi-a-mean', style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#636EFA'}),
        ], style={'padding': '16px', 'background': '#fff', 'borderRadius': '8px',
                  'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
        html.Div(children=[
            html.Div('指标 B 均值', style={'fontSize': '12px', 'color': '#888'}),
            html.Div(id='kpi-b-mean', style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#EF553B'}),
        ], style={'padding': '16px', 'background': '#fff', 'borderRadius': '8px',
                  'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
        html.Div(children=[
            html.Div('指标 C 均值', style={'fontSize': '12px', 'color': '#888'}),
            html.Div(id='kpi-c-mean', style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#00CC96'}),
        ], style={'padding': '16px', 'background': '#fff', 'borderRadius': '8px',
                  'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
    ], style={'display': 'flex', 'gap': '16px', 'padding': '16px 24px', 'background': '#f6f7f9'}),

    html.Div(children=[
        html.Div([dcc.Graph(id='timeseries-fig')],
                 style={'padding': '12px', 'background': '#fff', 'borderRadius': '8px',
                        'boxShadow': '0 1px 3px rgba(0,0,0,0.08)'}),
    ], style={'padding': '0 24px 16px 24px'}),

    html.Div(children=[
        html.Div([dcc.Graph(id='category-fig')],
                 style={'padding': '12px', 'background': '#fff', 'borderRadius': '8px',
                        'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
        html.Div([dcc.Graph(id='region-fig')],
                 style={'padding': '12px', 'background': '#fff', 'borderRadius': '8px',
                        'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
        html.Div([dcc.Graph(id='status-fig')],
                 style={'padding': '12px', 'background': '#fff', 'borderRadius': '8px',
                        'boxShadow': '0 1px 3px rgba(0,0,0,0.08)', 'flex': 1}),
    ], style={'display': 'flex', 'gap': '16px', 'padding': '0 24px 24px 24px'}),
], style={'minHeight': '100vh', 'background': '#f6f7f9', 'fontFamily': '-apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial'})


if __name__ == '__main__':
    _ensure_data()
    print("=" * 60)
    print("  Plotly Dash 动态仪表盘 + Flask 数据 API")
    print("=" * 60)
    print(f"  仪表盘界面:  http://localhost:8050/dashboard/")
    print(f"  数据接口:    http://localhost:8050/api/data")
    print(f"  健康检查:    http://localhost:8050/api/health")
    print()
    print("  /api/data 支持参数:")
    print("    ?category=Electronics   按类别过滤")
    print("    &region=North           按区域过滤")
    print("    &status=Active          按状态过滤")
    print("    &limit=50               限制时间序列条数")
    print("    &live=true              追加一条新数据后返回")
    print("    &refresh=true           重置全部数据")
    print("=" * 60)
    app.run(debug=False, host='0.0.0.0', port=8050)
