from dash import Dash, html, dcc, callback, Output, Input, dash_table
from datetime import datetime, timedelta
import plotly.express as px
import pandas as pd
import clickhouse_connect
import os

def ch_stats(cmd):
    def ch_df(limit=0):
        client = clickhouse_connect.get_client(
            host='clickhouse_server', 
            username=os.environ['CLICKHOUSE_USER'], 
            password=os.environ['CLICKHOUSE_PASSWORD']
        )
        if limit > 0:
            df = client.query_df(query=cmd + " LIMIT {}".format(limit))
        else:
            df = client.query_df(query=cmd)
        
        client.close()
        return df
    return ch_df

repo_ownership_stats = ch_stats(
    cmd="""
    SELECT
        repo_author,
        countDistinct(repo_name) AS repos_total
    FROM gharchive.repos
    GROUP BY repo_author
    HAVING repos_total > 1
    ORDER BY repos_total DESC
    """
)

commits_gt1_stats = ch_stats(
    cmd = """
    SELECT                                                                                                                                                                                                                                                                   
        date,
        author_name,
        count(*) AS total_commits
    FROM gharchive.commits
    GROUP BY
        date,
        author_name
    HAVING total_commits >= 2
    ORDER BY
        author_name ASC,
        total_commits DESC
    """
)

commits_dts_stats = ch_stats(
    cmd = """
    SELECT
        toDate(min(created_at)) AS min_created_at,
        toDate(max(created_at)) AS max_created_at
    FROM gharchive.commits
    """
)

members_gt10_stats = ch_stats(
    cmd = """
    SELECT
        repo_name,
        repo_name_full,
        countDistinct(member_login) AS total_members
    FROM gharchive.members
    GROUP BY
        repo_name,
        repo_name_full
    HAVING total_members > 10
    ORDER BY 
        total_members DESC,
        repo_name ASC
    """
)

members_basic_stats = ch_stats(
    cmd = """
    SELECT
        date,
        toHour(created_at) h,
        count(*) AS total
    FROM gharchive.members
    GROUP BY date, h
    ORDER BY date DESC, h DESC
    """
)

commits_basic_stats = ch_stats(
    cmd = """
    SELECT
        date,
        toHour(created_at) h,
        count(*) AS total
    FROM gharchive.commits
    GROUP BY date, h
    ORDER BY date DESC, h DESC
    """
)

repos_basic_stats = ch_stats(
    cmd = """
    SELECT
        date,
        count(*) AS total
    FROM gharchive.repos
    GROUP BY date
    ORDER BY date DESC
    """
)

## DASH!

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

def main_layout():
    return html.Div([
        html.H1(children='GitHub Archive Statistics', style={'textAlign':'center'}),
        html.P(children='Disclaimer: For performance reasons, only Top 100 records are shown.', style={'textAlign':'center'}),
        html.P(children='Refresh Interval: 1m.', style={'textAlign':'center'}),
        
        html.H2(children='Repos Total', style={'textAlign':'left'}),
        html.Div(id='live-update-repos'),
        
        html.H2(children='Commits Total', style={'textAlign':'left'}),
        html.Div(id='live-update-commits'),
        
        html.H2(children='Members Total', style={'textAlign':'left'}),
        html.Div(id='live-update-members'),
    
        dcc.Interval(
            id='interval-component',
            interval=1*60*1000, # in milliseconds
            n_intervals=0
        )
    ])

app.layout = main_layout()

@callback(
    Output('live-update-repos', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_repos_df(n):
    df_basic = repos_basic_stats()
    df = repo_ownership_stats(limit=100)

    if len(df_basic) == 0:
        return html.P(children='Nothing to show. The repos table is empty!', style={'textAlign':'center'})
    else:
        return [
            html.H3(children='Repo Owners with >= 2 Repos', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                dash_table.DataTable(
                    data=df.to_dict('records'), 
                    page_size=10,
                    columns=[{"name": i, "id": i} for i in df.columns], 
                    style_table={'overflowX': 'auto'}
                ),
            ]),
            html.H3(children='Basic Repos Table Stats', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                html.Div(className='six columns', children=[
                    dcc.Graph(id='graph-bars-repos', figure = px.bar(df_basic.groupby('date')['total'].sum().reset_index(), x='date', y='total')),
                ]),
                html.Div(className='six columns', children=[
                    dash_table.DataTable(
                        data=df_basic.to_dict('records'), 
                        page_size=10,
                        columns=[{"name": i, "id": i} for i in df_basic.columns], 
                        style_table={'overflowX': 'auto'}
                    ),
                ]),
            ])
        ]

@callback(
    Output('live-update-commits', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_commits_df(n):
    df_basic = repos_basic_stats()
    df_gt = commits_gt1_stats(limit=100)
    df_dates = commits_dts_stats()
    commits_lt1_stats = ch_stats(
        cmd = """
        SELECT t.author_name, t.interval_start_dt
        FROM
        (
            SELECT
                author_name,
                created_at,
                1 AS total,
                toStartOfInterval(created_at, toIntervalDay(1)) AS interval_start_dt,
                sum(total) OVER (PARTITION BY author_name, interval_start_dt ORDER BY created_at ASC) AS sum_commits
            FROM gharchive.commits
            ORDER BY
                author_name ASC,
                interval_start_dt ASC 
                    WITH FILL 
                    FROM toUnixTimestamp('{}') 
                    TO toUnixTimestamp('{}') 
                    STEP toIntervalDay(1)
        ) AS t
        WHERE sum_commits = 0
        """.format(
            df_dates['min_created_at'][0].strftime("%F"),
            (df_dates['max_created_at'][0] + timedelta(days=1)).strftime("%F")
        )
    )
    df_lt = commits_lt1_stats(limit=100)

    if len(df_basic) == 0:
        return html.P(children='Nothing to show. The commits table is empty!', style={'textAlign':'center'})
    else:
        return [
            html.H3(children='Developers with > 1 commit in a day', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                dash_table.DataTable(
                    data=df_gt.to_dict('records'), 
                    page_size=10,
                    columns=[{"name": i, "id": i} for i in df_gt.columns], 
                    style_table={'overflowX': 'auto'}
                ),
            ]),
            html.H3(children='Developers with < 1 commit in a day', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                dash_table.DataTable(
                    data=df_lt.to_dict('records'), 
                    page_size=10,
                    columns=[{"name": i, "id": i} for i in df_lt.columns], 
                    style_table={'overflowX': 'auto'}
                ),
            ]),
            html.H3(children='Basic Commits Table Stats', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                html.Div(className='six columns', children=[
                    dcc.Graph(id='graph-bars-commits', figure = px.bar(df_basic.groupby('date')['total'].sum().reset_index(), x='date', y='total')),
                ]),
                html.Div(className='six columns', children=[
                    dash_table.DataTable(
                        data=df_basic.to_dict('records'), 
                        page_size=10,
                        columns=[{"name": i, "id": i} for i in df_basic.columns], 
                        style_table={'overflowX': 'auto'}
                    ),
                ]),
            ])
        ]
    

@callback(
    Output('live-update-members', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_members_df(n):
    df_basic = members_basic_stats()
    df = members_gt10_stats(limit=100)

    if len(df_basic) == 0:
        return html.P(children='Nothing to show. The members table is empty!', style={'textAlign':'center'})
    else:
        return [
            html.H3(children='Total Projects with > 10 Members: {}'.format(df['repo_name_full'].count()), style={'textAlign':'center'}),
            html.Div(className='row', children=[
                dash_table.DataTable(
                    data=df.to_dict('records'), 
                    page_size=10,
                    columns=[{"name": i, "id": i} for i in df.columns], 
                    style_table={'overflowX': 'auto'}
                ),
            ]),
            html.H3(children='Basic Members Table Stats', style={'textAlign':'center'}),
            html.Div(className='row', children=[
                html.Div(className='six columns', children=[
                    dcc.Graph(id='graph-bars-members', figure = px.bar(df_basic.groupby('date')['total'].sum().reset_index(), x='date', y='total')),
                ]),
                html.Div(className='six columns', children=[
                    dash_table.DataTable(
                        data=df_basic.to_dict('records'), 
                        page_size=10,
                        columns=[{"name": i, "id": i} for i in df_basic.columns], 
                        style_table={'overflowX': 'auto'}
                    ),
                ]),
            ])
        ]
    
if __name__ == '__main__':
    app.run(
        # debug=True, 
        host="0.0.0.0", 
        port=8050
    )
