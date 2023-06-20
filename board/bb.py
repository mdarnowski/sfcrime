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
        self.graph_type = graph_type
        self.db_manager = PostgreSQLManager.get_instance()

    def get_data(self, query):
        return pd.read_sql(query.statement, self.db_manager.Session.bind)

    def plot_graph(self, df):
        if self.graph_type == 'bar':
            fig = px.bar(df, y='incident_category', x='num_of_incidents',
                         title='Incident Analysis',
                         labels={'incident_category': 'Incident Category', 'num_of_incidents': 'Number of Incidents'},
                         text='num_of_incidents',
                         color='num_of_incidents',
                         color_continuous_scale=px.colors.sequential.Plasma)

            fig.update_layout(title_font=dict(size=26, color='darkblue', family="Arial, sans-serif"),
                              xaxis=dict(title_font=dict(size=18, color='darkred')),
                              yaxis=dict(title_font=dict(size=18, color='darkgreen')),
                              legend=dict(title_font=dict(size=16), title_text='Categories'),
                              font=dict(family="Arial, sans-serif"),
                              uniformtext_minsize=8,
                              uniformtext_mode='hide',
                              template='plotly_white',
                              uirevision='constant')

        elif self.graph_type == 'stacked_bar':
            fig = px.bar(df, y='incident_category', x='num_of_incidents',
                         color='resolution',
                         title='Overview of Resolution Status Across Crime Categories',
                         labels={'incident_category': 'Crime Category', 'num_of_incidents': 'Number of Incidents',
                                 'resolution': 'Resolution'},
                         height=600,
                         template='plotly_white')
        return fig


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
    query_plotter = QueryPlotter(graph_type)

    if graph_type == 'bar':
        query = PostgreSQLManager.get_instance().Session.query(CategoryDimension.incident_category,
                                                               func.count(Incidents.incident_id).label('num_of_incidents')) \
            .join(Incidents, Incidents.category_key == CategoryDimension.key) \
            .group_by(CategoryDimension.incident_category)
    elif graph_type == 'stacked_bar':
        query = PostgreSQLManager.get_instance().Session.query(CategoryDimension.incident_category,
                                                               ResolutionDimension.resolution,
                                                               func.count(Incidents.incident_id).label('num_of_incidents')) \
            .join(Incidents, Incidents.category_key == CategoryDimension.key) \
            .join(ResolutionDimension, ResolutionDimension.key == Incidents.resolution_key) \
            .group_by(CategoryDimension.incident_category, ResolutionDimension.resolution)

    df = query_plotter.get_data(query)
    fig = query_plotter.plot_graph(df)
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
