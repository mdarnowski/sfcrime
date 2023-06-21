import numpy as np
import pandas as pd
from plotly import express as px
import plotly.graph_objects as go
from utilities.PostgreSQLManager import PostgreSQLManager
from datetime import datetime, date
import statsmodels


class QueryPlotter:
    def __init__(self, graph_type):
        """
        Initialize the QueryPlotter object with graph_type.

        :param graph_type: Type of graph ('bar' or 'stacked_bar').
        """
        self.graph_config = GRAPH_CONFIG[graph_type]
        self.db_manager = PostgreSQLManager.get_instance()

    def plot_graph(self):
        """
        Plot the graph based on the data provided.

        :return: Plotly figure object.
        """
        df = self.get_data()
        fig = getattr(self, self.graph_config['plot_func'])(df, **self.graph_config.get('plot_params', {}))
        if self.graph_config.get('is_logarithmic', False):
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

    def get_data(self, *args, **kwargs):
        """
        Fetch data from the database.

        :return: DataFrame containing the result of the query.
        """
        return getattr(self.db_manager, self.graph_config['query_func'])(*args, **kwargs)

    def plot_bar_graph(self, df, x, y, color, title, labels):
        """
        Plot a bar graph.

        :param df: DataFrame containing data for the graph.
        :param x: Column name for the x-axis.
        :param y: Column name for the y-axis.
        :param color: Column name for color encoding.
        :param title: Title of the graph.
        :param labels: Dict of labels for the axes and legend.
        :return: Plotly figure object.
        """
        hover_data = {k: True for k in df.columns}  # Include all columns as hover data
        fig = px.bar(df, y=y, x=x, title=title, labels=labels, text=x, color=color,
                     color_continuous_scale=px.colors.sequential.Plasma, hover_data=hover_data)

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

    def plot_scatter_graph(self, df):
        """
        Plot a heat map on a map of San Francisco.

        :param df: DataFrame containing data for the graph.
        :return: Plotly figure object.
        """
        fig = go.Figure(go.Densitymapbox(
            lat=df['latitude'],
            lon=df['longitude'],
            z=df['num_of_incidents'],  # this will create a heat map based on the number of incidents
            radius=20,  # increase the size of the heat map points
            colorscale='Hot',  # change the color scale to 'Hot'
            colorbar=dict(thickness=20, ticklen=3),  # customize the colorbar
            hoverinfo='text',  # this will display the number of incidents when you hover over a point
            hovertext=df['num_of_incidents'].astype(str) + ' incidents'  # customize the hover text
        ))

        fig.update_layout(
            autosize=True,
            title='Crime Density Map of San Francisco',
            hovermode='closest',
            mapbox=dict(
                accesstoken='pk.eyJ1IjoiczE2OTQxIiwiYSI6ImNsajVvMG4wMDBjcGYzY3F5OWJjazcxMmgifQ.lCzhgVuM5B6GOufCDaomBw',
                # replace with your Mapbox access token
                bearing=0,
                center=dict(
                    lat=37.7749,  # latitude of San Francisco
                    lon=-122.4194  # longitude of San Francisco
                ),
                pitch=0,
                zoom=10
            ),
            height=800,  # adjust the height of the map here
        )
        fig.update_layout(self.get_shared_layout())

        return fig

    def plot_line_graph(self, df):
        """
        Plot a line graph connecting dots of the same category, grouped by 60 minutes.

        :param df: DataFrame containing data for the graph.
        :return: Plotly figure object.
        """

        # Convert incident_time to datetime
        df['incident_time'] = pd.to_datetime(df['incident_time'].apply(lambda x: datetime.combine(date.today(), x)))
        df.set_index('incident_time', inplace=True)  # Set incident_time as the index
        grouped_df = df.groupby(['incident_category', pd.Grouper(freq='60T')]).sum().reset_index()

        fig = go.Figure()

        # Iterate over each category and add a line trace
        for category, group in grouped_df.groupby('incident_category'):
            fig.add_trace(go.Scatter(
                x=group['incident_time'],
                y=group['num_of_incidents'],
                mode='lines',
                name=category,
                line=dict(shape='spline'),
                marker=dict(size=6),
            ))

        fig.update_layout(
            title='Temporal Crime Trends by Hour of the Day',
            xaxis=dict(
                title='Time of the Day',
                tickmode='array',
                tickvals=pd.date_range(start=date.today(), periods=24, freq='H').tolist(),
                ticktext=[t.strftime('%H:%M') for t in pd.date_range(start=date.today(), periods=24, freq='H')],
                tickformat='%H:%M',
            ),
            yaxis_title='Number of Incidents',
            template='plotly_white',
            showlegend=True
        )

        fig.update_layout(self.get_shared_layout())

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


GRAPH_CONFIG = {
    'crime_hotspots': {
        'label': 'Crime Hotspots',
        'query_func': 'fetch_crime_hotspots',
        'plot_func': 'plot_scatter_graph',
    },
    'incident_analysis': {
        'label': 'Incident Analysis',
        'query_func': 'fetch_category_counts',
        'is_logarithmic': True,
        'plot_func': 'plot_bar_graph',
        'plot_params': {
            'x': 'num_of_incidents',
            'y': 'incident_category',
            'color': 'num_of_incidents',
            'title': 'Incident Analysis',
            'labels': {'incident_category': 'Incident Category', 'num_of_incidents': 'Number of Incidents'}
        }
    },
    'resolution_status': {
        'label': 'Resolution Status Across Crime Categories',
        'is_logarithmic': True,
        'query_func': 'fetch_category_resolution_counts',
        'plot_func': 'plot_stacked_bar_graph',
    },
    'most_frequent_crimes': {
        'label': 'Most Frequent Crimes',
        'query_func': 'fetch_most_frequent_crimes',
        'is_logarithmic': True,
        'plot_func': 'plot_bar_graph',
        'plot_params': {
            'x': 'num_of_incidents',
            'y': 'incident_category',
            'color': 'num_of_incidents',
            'title': 'Most Frequent Crimes',
            'labels': {'incident_category': 'Crime Type', 'num_of_incidents': 'Frequency'}
        }
    },
    'crime_trends': {
        'label': 'Temporal Crime Trends',
        'query_func': 'fetch_crime_trends',
        'plot_func': 'plot_line_graph'
    },
    'district_crimes': {
        'label': 'Crimes in Specific Districts',
        'query_func': 'fetch_district_crimes',
        'is_logarithmic': True,
        'plot_func': 'plot_bar_graph',
        'plot_params': {
            'x': 'num_of_incidents',
            'y': 'police_district',
            'color': 'num_of_incidents',
            'title': 'Crimes in Specific Districts',
            'labels': {'police_district': 'Police District', 'num_of_incidents': 'Number of Incidents','incident_category': 'Incident Category'}
        }
    }
}
