import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy.sql import func
from model.SQLAlchemy import Incidents, CategoryDimension, ResolutionDimension
from utilities.PostgreSQLManager import PostgreSQLManager


class QueryPlotter:
    def __init__(self, graph_type):
        """
        Initialize the QueryPlotter object with graph_type.

        :param graph_type: Type of graph ('bar' or 'stacked_bar').
        """
        self.graph_type = graph_type
        self.db_manager = PostgreSQLManager.get_instance()

    def get_data(self, query):
        """
        Fetch data from the database.

        :param query: SQLAlchemy query object.
        :return: DataFrame containing the result of the query.
        """
        return pd.read_sql(query.statement, self.db_manager.Session.bind)

    def plot_graph(self, df):
        """
        Plot the graph based on the data provided.

        :param df: DataFrame containing data for the graph.
        :return: Plotly figure object.
        """
        if self.graph_type == 'bar':
            fig = self.plot_bar_graph(df)
        elif self.graph_type == 'stacked_bar':
            fig = self.plot_stacked_bar_graph(df)

        # Add annotation for logarithmic scale
        fig.add_annotation(
            x=0,
            y=1.05,
            xref="paper",
            yref="paper",
            text="Note: The x-axis is on a logarithmic scale.",
            showarrow=False,
            font=dict(size=12, color="red")
        )

        return fig

    def plot_bar_graph(self, df):
        """
        Plot a bar graph.

        :param df: DataFrame containing data for the graph.
        :return: Plotly figure object.
        """
        fig = px.bar(df, y='incident_category', x='num_of_incidents',
                     title='Incident Analysis',
                     labels={'incident_category': 'Incident Category', 'num_of_incidents': 'Number of Incidents'},
                     text='num_of_incidents',
                     color='num_of_incidents',
                     color_continuous_scale=px.colors.sequential.Plasma)

        fig.update_layout(self.get_shared_layout())
        fig.update_layout(xaxis=dict(type='log'))
        return fig

    def plot_stacked_bar_graph(self, df):
        """
        Plot a stacked bar graph.

        :param df: DataFrame containing data for the graph.
        :return: Plotly figure object.
        """
        fig = px.bar(df, y='incident_category', x='num_of_incidents',
                     color='resolution',
                     title='Overview of Resolution Status Across Crime Categories',
                     labels={'incident_category': 'Crime Category', 'num_of_incidents': 'Number of Incidents',
                             'resolution': 'Resolution'},
                     template='plotly_white')

        fig.update_layout(self.get_shared_layout())
        fig.update_layout(xaxis=dict(type='log'))
        return fig

    @staticmethod
    def get_shared_layout():
        """
        Shared layout properties for graphs.

        :return: Dictionary containing shared layout properties.
        """
        return dict(title_font=dict(size=26, color='darkblue', family="Arial, sans-serif"),
                    xaxis=dict(title_font=dict(size=18, color='darkred')),
                    yaxis=dict(title_font=dict(size=18, color='darkgreen')),
                    legend=dict(title_font=dict(size=16), title_text='Categories'),
                    height=600,
                    uniformtext_minsize=8,
                    uniformtext_mode='hide',
                    template='plotly_white',
                    uirevision='constant')


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Setting up the layout of the dashboard
app.layout = dbc.Container([
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


@app.callback(
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
    app.run_server(debug=True)
