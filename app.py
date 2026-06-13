import datetime
import json
import random
import threading
import time

import numpy as np
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, callback
from flask import Flask, jsonify, request
from plotly import graph_objs as go

server = Flask(__name__)
app = Dash(__name__, server=server, url_base_pathname='/dashboard/')

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


@server.route('/api/data', methods=['GET'])
def api_data():
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    if force_refresh:
        with _lock:
            for key in list(_data_cache.keys()):
                if key != 'generated_at':
                    _data_cache[key] = []
            _data_cache['generated_at'] = None
        _ensure_data()
    if request.args.get('live', 'false').lower() == 'true':
        _append_new_point()
    df = build_dataframe()

    category = request.args.get('category')
    region = request.args.get('region')
    status = request.args.get('status')
    if category:
        df = df[df['category'] == category]
    if region:
        df = df[df['region'] == region]
    if status:
        df = df[df['status'] == status]

    limit_raw = request.args.get('limit')
    limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None

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
    return jsonify(response)


@server.route('/api/health', methods=['GET'])
def api_health():
    return jsonify({'status': 'ok', 'time': _iso(datetime.datetime.now())})


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
    Input('metric-select', 'value'),
    State('live-switch', 'on'),
)
def update_dashboard(n, metrics, live):
    if live:
        _append_new_point()
    df = build_dataframe()
    stats = summary_stats(df)

    kpi_count = f"{stats['count']:,}"
    kpi_a = f"{stats['metric_a_mean']:.2f}"
    kpi_b = f"{stats['metric_b_mean']:.2f}"
    kpi_c = f"{stats['metric_c_mean']:.2f}"
    last_update = f"最后更新: {_iso(datetime.datetime.now())}"

    metric_cols = metrics or ['metric_a', 'metric_b', 'metric_c']
    ts_fig = go.Figure()
    palette = {'metric_a': '#636EFA', 'metric_b': '#EF553B', 'metric_c': '#00CC96'}
    labels = {'metric_a': '指标 A (响应)', 'metric_b': '指标 B (吞吐)', 'metric_c': '指标 C (负载)'}
    for col in metric_cols:
        ts_fig.add_trace(go.Scatter(
            x=df['timestamp'], y=df[col], mode='lines', name=labels.get(col, col),
            line=dict(color=palette.get(col, None), width=2),
        ))
    ts_fig.update_layout(
        title='指标时间序列',
        xaxis_title='时间', yaxis_title='数值',
        template='plotly_white', margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )

    cat_df = pd.DataFrame(aggregate_by_category(df))
    cat_fig = go.Figure()
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

    reg_df = pd.DataFrame(aggregate_by_region(df))
    reg_fig = go.Figure()
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

    sts_df = pd.DataFrame(aggregate_by_status(df))
    status_colors = {'Active': '#00CC96', 'Inactive': '#AB63FA',
                     'Pending': '#FFA15A', 'Failed': '#EF553B'}
    sts_fig = go.Figure()
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
    dcc.Interval(id='interval-component', interval=3000, n_intervals=0),
    html.Header(children=[
        html.H1('动态数据仪表盘', style={'margin': 0, 'fontSize': '24px'}),
        html.Div(id='last-update', style={'marginTop': '6px', 'fontSize': '13px', 'color': '#666'}),
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
        ], style={'flex': 3}),
        html.Div([
            dcc.Checklist(
                id='live-switch',
                options=[{'label': ' 实时推送数据', 'value': 'on'}],
                value=['on'],
                labelStyle={'fontSize': '14px'},
            ),
        ], style={'flex': 1, 'textAlign': 'right'}),
    ], style={'padding': '12px 24px', 'display': 'flex', 'alignItems': 'center',
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
