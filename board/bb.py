import dash
import dash_bootstrap_components as dbc
from dash import dcc
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from config.database import db_config
from model.SQLAlchemy import Base, Incidents, CategoryDimension

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Setting up SQLAlchemy session
engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
Session = sessionmaker(bind=engine)
session = Session()

# Querying the data for the chart
df = pd.read_sql(session.query(CategoryDimension.incident_category,
                               func.count(Incidents.incident_id).label('num_of_incidents'))
                 .join(Incidents, Incidents.category_key == CategoryDimension.key)
                 .group_by(CategoryDimension.incident_category)
                 .statement, session.bind)

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
