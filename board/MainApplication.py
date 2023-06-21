from flask import Flask, render_template, jsonify, Response
from BatchInserter import InsertTask, ActionLock
from utilities.PostgreSQLManager import PostgreSQLManager
import json
import time
import threading
from dash import html, dcc, Dash
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from utilities.QueryPlotter import QueryPlotter, GRAPH_CONFIG

app = Flask(__name__)
dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')
app.template_folder = 'templates'
action_lock = ActionLock()
task = InsertTask()


@app.route('/')
def index():
    """Handle index page requests."""
    return render_template('index.html')


@app.route('/insert_batches', methods=['POST'])
def handle_insert_batches():
    """
    Start batch insertion into database if no other task is running.
    """
    if not action_lock.is_locked() and not task.running:
        action_lock.perform(lambda: threading.Thread(target=task.run).start())
    return jsonify({"message": "Batch Insertion started"}), 202


@app.route('/create_database', methods=['POST'])
def handle_create_database():
    """
    Start creating database if no other task is running.
    """
    if not action_lock.is_locked() and not task.running:
        action_lock.perform(
            lambda: PostgreSQLManager.get_instance().create_database())
    return jsonify({"message": "Database creation started"}), 202


@app.route('/recreate_tables', methods=['POST'])
def handle_recreate_tables():
    """
    Start recreating tables in database if no other task is running.
    """
    if not action_lock.is_locked() and not task.running:
        action_lock.perform(
            lambda: PostgreSQLManager.get_instance().recreate_tables())
    return jsonify({"message": "Table recreation started"}), 202


@app.route('/stream_updates')
def stream_updates():
    """
    Stream updates about task progress to client.
    """
    def generate():
        while True:
            if task.running:
                yield f"data:{json.dumps({'total_rows_added': task.total_rows_added,'progress': task.progress})}\n\n"
            else:
                break
            time.sleep(1)

    return Response(generate(), mimetype='text/event-stream')


# Dash app layout
dash_app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(id='graph-dropdown',
                         options=[{'label': cfg['label'], 'value': key}
                                  for key, cfg in GRAPH_CONFIG.items()],
                         value='incident_analysis',
                         clearable=False),
            dcc.Graph(id='incident-graph'),
            html.A(
                html.Button(
                    'Initialization menu',
                    className='go-to-flask-app',
                    style={
                        'border': 'none',
                        'padding': '10px 20px',
                        'marginBottom': '10px',
                        'borderRadius': '5px',
                        'color': '#fff',
                        'cursor': 'pointer',
                        'fontSize': '16px',
                        'background': '#6c757d',
                        'width': '200px'
                    }
                ),
                href='/',
                style={
                    'display': 'flex',
                    'justifyContent': 'center',
                    'margin': '10px auto'
                }
            ),
        ], width=12)
    ])
], fluid=True)


@dash_app.callback(
    Output('incident-graph', 'figure'),
    Input('graph-dropdown', 'value')
)
def update_graph(graph_type):
    """
    Callback function to update the graph based on the dropdown selection.

    :param graph_type: Type of graph selected.
    :return: Plotly figure object.
    """
    query_plotter = QueryPlotter(graph_type)
    return query_plotter.plot_graph()


if __name__ == '__main__':
    dash_app.run_server(debug=True)
