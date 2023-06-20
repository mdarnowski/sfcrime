from flask import Flask, render_template, jsonify, Response
from BatchInserter import InsertTask, ActionLock
from utilities.PostgreSQLManager import PostgreSQLManager
import json
import time
import threading
from dash import dcc, Dash
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from utilities.QueryPlotter import QueryPlotter

app = Flask(__name__)
dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')
app.template_folder = 'templates'

action_lock = ActionLock()

task = InsertTask()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/insert_batches', methods=['POST'])
def handle_insert_batches():
    if not action_lock.is_locked():
        action_lock.perform(lambda: threading.Thread(target=task.run).start())
    return jsonify({"message": "Batch Insertion started"}), 202


@app.route('/create_database', methods=['POST'])
def handle_create_database():
    if not action_lock.is_locked():
        action_lock.perform(lambda: PostgreSQLManager.get_instance().create_database())
    return jsonify({"message": "Database creation started"}), 202


@app.route('/recreate_tables', methods=['POST'])
def handle_recreate_tables():
    if not action_lock.is_locked():
        action_lock.perform(lambda: PostgreSQLManager.get_instance().recreate_tables())
    return jsonify({"message": "Table recreation started"}), 202


@app.route('/stream_updates')
def stream_updates():
    def generate():
        while True:
            if task.running:
                yield f"data:{json.dumps({'total_rows_added': task.total_rows_added, 'progress': task.progress})}\n\n"
            else:
                break
            time.sleep(1)

    return Response(generate(), mimetype='text/event-stream')


# Dash app layout
dash_app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(id='graph-dropdown',
                         options=[
                             {'label': 'Incident Analysis', 'value': 'bar'},
                             {'label': 'Resolution Status Across Crime Categories', 'value': 'stacked_bar'}
                         ],
                         value='bar',
                         clearable=False),
            dcc.Graph(id='incident-graph')
        ], width=12)
    ])
], fluid=True)


# Dash app callback
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
    db_manager = PostgreSQLManager.get_instance()
    if graph_type == 'bar':
        df = db_manager.fetch_category_counts()
    elif graph_type == 'stacked_bar':
        df = db_manager.fetch_category_resolution_counts()
    query_plotter = QueryPlotter(graph_type)
    fig = query_plotter.plot_graph(df)
    return fig


if __name__ == '__main__':
    dash_app.run_server(debug=True)
