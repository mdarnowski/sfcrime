from dash.dcc import Loading
from dash.exceptions import PreventUpdate
from flask import Flask, render_template, jsonify, Response
from utilities.PostgreSQL_BatchInserter import InsertTask, ActionLock
from utilities.PostgreSQLManager import PostgreSQLManager
import json
import time
import threading
from dash import html, dcc, Dash
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from utilities.QueryPlotter import QueryPlotter, GRAPH_CONFIG


def create_app():
    app = Flask(__name__)
    dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')
    app.template_folder = 'templates'
    action_lock = ActionLock()
    task = InsertTask()

    @app.route('/')
    def index():
        """
        Serve the index page.

        This function handles requests for the root URL ("/") and renders the 'index.html' template.

        :return: Rendered 'index.html' template.
        """
        return render_template('index.html')

    def another_db_process_msg():
        return jsonify({
            "message": "Currently, another database process is being executed"
        }), 503

    @app.route('/insert_batches', methods=['POST'])
    def handle_insert_batches():
        """
        Handle requests to start batch insertion into the database.

        If no other task is currently running, this function starts a new thread for batch insertion into the database.

        :return: JSON response with a success message and HTTP status 202 if batch insertion has started,
                 or JSON response with an error message and HTTP status 503 if another task is running.
        """
        if not action_lock.is_locked() and not task.running:
            action_lock.perform(lambda: threading.Thread(target=task.run).start())
            return jsonify({"message": "Batch Insertion started"}), 202
        return another_db_process_msg()

    @app.route('/create_database', methods=['POST'])
    def handle_create_database():
        """
        Handle requests to start creating the database.

        If no other task is currently running, this function starts the database creation.

        :return: JSON response with a success message and HTTP status 202 if database creation has started,
                 or JSON response with an error message and HTTP status 503 if another task is running.
        """
        if not action_lock.is_locked() and not task.running:
            action_lock.perform(
                lambda: PostgreSQLManager.get_instance().create_database())
            return jsonify({"message": "Database creation started"}), 202
        return another_db_process_msg()

    @app.route('/recreate_tables', methods=['POST'])
    def handle_recreate_tables():
        """
        Handle requests to start recreating the tables in the database.

        If no other task is currently running, this function starts the tables recreation in the database.

        :return: JSON response with a success message and HTTP status 202 if table recreation has started,
                 or JSON response with an error message and HTTP status 503 if another task is running.
        """
        if not action_lock.is_locked() and not task.running:
            action_lock.perform(
                lambda: PostgreSQLManager.get_instance().recreate_tables())
            return jsonify({"message": "Table recreation started"}), 202
        return another_db_process_msg()

    @app.route('/stream_updates')
    def stream_updates():
        """
        Stream updates about the task progress to the client.

        This function generates a server-sent event stream that sends updates about the task progress to the client.

        :return: Server-sent event stream response.
        """

        def generate():
            while True:
                yield f"data:{json.dumps({'total_rows_added': task.total_rows_added, 'progress': task.progress})}\n\n"
                if not task.running:
                    break
                time.sleep(1)

        return Response(generate(), mimetype='text/event-stream')

    # Dash app layout
    dash_app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(id='db-type-dropdown',
                             options=[
                                 {'label': 'SQL Database', 'value': 0},
                                 {'label': 'Cassandra Database', 'value': 1}
                             ],
                             value=1,  # Default value
                             clearable=False,
                             style={'width': '50%', 'marginBottom': '20px'}
                             ),
                dcc.Dropdown(id='graph-dropdown',
                             options=[{'label': cfg['label'], 'value': key}
                                      for key, cfg in GRAPH_CONFIG.items()],
                             value='crime_hotspots',
                             clearable=False),
                Loading(
                    id="loading-1",
                    type="default",
                    children=dcc.Graph(id="incident-graph")  # Include dcc.Graph here
                ),
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
        [Input('graph-dropdown', 'value'),
         Input('db-type-dropdown', 'value')]
    )
    def update_graph(graph_type, db_type):
        """
        Update the graph based on the dropdown selection.

        :param db_type: type of database: 0 - postgres-sql, 1 - cassandra
        :param graph_type: Type of graph selected in the dropdown.
        :return: Plotly figure object for the selected graph type.
        """
        if graph_type is None or db_type is None:
            raise PreventUpdate

        query_plotter = QueryPlotter(graph_type, db_type)
        return query_plotter.plot_graph()
    return app
