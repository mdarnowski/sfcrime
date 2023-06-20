import dash
import dash_bootstrap_components as dbc
from dash import dcc
import plotly.express as px
import pandas as pd
from sqlalchemy.sql import func
from model.SQLAlchemy import Incidents, CategoryDimension
from utilities.PostgreSQLManager import PostgreSQLManager

db_manager = PostgreSQLManager.get_instance()
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Querying the data for the chart
df = pd.read_sql(db_manager.Session.query(CategoryDimension.incident_category,
                                          func.count(Incidents.incident_id).label('num_of_incidents'))
                 .join(Incidents, Incidents.category_key == CategoryDimension.key)
                 .group_by(CategoryDimension.incident_category)
                 .statement, db_manager.Session.bind)

# Creating the chart
fig = px.bar(df, x='incident_category', y='num_of_incidents', title='Number of incidents per category')

# Setting up the layout of the dashboard
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig)
        ])
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True)
