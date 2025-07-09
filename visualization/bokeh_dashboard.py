import pandas as pd
import numpy as np
from bokeh.plotting import figure, show, curdoc
from bokeh.models import ColumnDataSource, HoverTool, Select, DateRangeSlider, CheckboxGroup
from bokeh.layouts import column, row, gridplot
from bokeh.palettes import Category20, Set3
from bokeh.io import output_notebook, push_notebook
from bokeh.transform import factor_cmap
from bokeh.models.widgets import Div, DataTable, TableColumn
from datetime import datetime, timedelta
import asyncio

class ParkingVisualizationDashboard:
    """Comprehensive Bokeh dashboard for parking pricing analysis"""
    
    def __init__(self, results_df=None):
        self.results_df = results_df
        self.colors = Category20[20]
        self.model_colors = {'baseline': '#1f77b4', 'demand': '#ff7f0e', 'competitive': '#2ca02c'}
        self.sources = {}
        self.plots = {}
        
        if results_df is not None:
            self.prepare_data()
    
    def prepare_data(self):
        """Prepare data sources for Bokeh plotting"""
        # Create sources for each model
        self.sources['baseline'] = ColumnDataSource(data=dict(
            timestamp=[], space_id=[], price=[], occupancy_rate=[]
        ))
        
        self.sources['demand'] = ColumnDataSource(data=dict(
            timestamp=[], space_id=[], price=[], occupancy_rate=[]
        ))
        
        self.sources['competitive'] = ColumnDataSource(data=dict(
            timestamp=[], space_id=[], price=[], occupancy_rate=[]
        ))
        
        # Aggregate data for comparison
        self.create_comparison_data()
    
    def create_comparison_data(self):
        """Create aggregated data for model comparison"""
        if self.results_df is None:
            return
        
        # Calculate average prices by timestamp
        comparison_data = self.results_df.groupby('timestamp').agg({
            'baseline_price': 'mean',
            'demand_price': 'mean',
            'competitive_price': 'mean',
            'occupancy_rate': 'mean'
        }).reset_index()
        
        self.comparison_source = ColumnDataSource(comparison_data)
        
        # Space-wise analysis
        space_analysis = self.results_df.groupby('space_id').agg({
            'baseline_price': 'mean',
            'demand_price': 'mean',
            'competitive_price': 'mean',
            'occupancy_rate': 'mean'
        }).reset_index()
        
        self.space_analysis_source = ColumnDataSource(space_analysis)
    
    def create_time_series_plot(self, width=1200, height=400):
        """Create time series plot for all models"""
        p = figure(
            title="Dynamic Pricing Models - Time Series Comparison",
            x_axis_label='Time',
            y_axis_label='Average Price ($)',
            width=width,
            height=height,
            x_axis_type='datetime',
            tools="pan,wheel_zoom,box_zoom,reset,save"
        )
        
        # Add lines for each model
        p.line('timestamp', 'baseline_price', source=self.comparison_source,
               line_width=3, alpha=0.8, color=self.model_colors['baseline'],
               legend_label='Baseline Model')
        
        p.line('timestamp', 'demand_price', source=self.comparison_source,
               line_width=3, alpha=0.8, color=self.model_colors['demand'],
               legend_label='Demand-Based Model')
        
        p.line('timestamp', 'competitive_price', source=self.comparison_source,
               line_width=3, alpha=0.8, color=self.model_colors['competitive'],
               legend_label='Competitive Model')
        
        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ('Time', '@timestamp{%F %T}'),
                ('Baseline Price', '@baseline_price{$0.00}'),
                ('Demand Price', '@demand_price{$0.00}'),
                ('Competitive Price', '@competitive_price{$0.00}'),
                ('Avg Occupancy', '@occupancy_rate{0.0%}')
            ],
            formatters={'@timestamp': 'datetime'}
        )
        p.add_tools(hover)
        
        # Configure legend
        p.legend.location = "top_left"
        p.legend.click_policy = "hide"
        
        return p
    
    def create_space_wise_plot(self, width=800, height=500):
        """Create space-wise comparison plot"""
        spaces = [str(i) for i in sorted(self.results_df['space_id'].unique())]
        
        p = figure(
            title="Average Pricing by Parking Space",
            x_range=spaces,
            y_axis_label='Average Price ($)',
            width=width,
            height=height,
            tools="pan,wheel_zoom,box_zoom,reset,save"
        )
        
        # Create grouped bar chart
        space_data = self.results_df.groupby('space_id').agg({
            'baseline_price': 'mean',
            'demand_price': 'mean',
            'competitive_price': 'mean'
        }).reset_index()
        
        x_baseline = [str(i) + ':0.8' for i in space_data['space_id']]
        x_demand = [str(i) + ':1.0' for i in space_data['space_id']]
        x_competitive = [str(i) + ':1.2' for i in space_data['space_id']]
        
        p.vbar(x=x_baseline, top=space_data['baseline_price'], width=0.2,
               color=self.model_colors['baseline'], alpha=0.8, legend_label='Baseline')
        
        p.vbar(x=x_demand, top=space_data['demand_price'], width=0.2,
               color=self.model_colors['demand'], alpha=0.8, legend_label='Demand-Based')
        
        p.vbar(x=x_competitive, top=space_data['competitive_price'], width=0.2,
               color=self.model_colors['competitive'], alpha=0.8, legend_label='Competitive')
        
        p.legend.location = "top_left"
        p.xaxis.axis_label = "Parking Space ID"
        
        return p
    
    def create_occupancy_price_scatter(self, width=600, height=400):
        """Create scatter plot of occupancy vs price"""
        p = figure(
            title="Occupancy Rate vs Price Correlation",
            x_axis_label='Occupancy Rate (%)',
            y_axis_label='Price ($)',
            width=width,
            height=height,
            tools="pan,wheel_zoom,box_zoom,reset,save"
        )
        
        # Sample data to avoid overcrowding
        sample_data = self.results_df.sample(n=min(1000, len(self.results_df)))
        
        # Create scatter plots for each model
        p.scatter(sample_data['occupancy_rate'] * 100, sample_data['baseline_price'],
                  size=8, alpha=0.6, color=self.model_colors['baseline'],
                  legend_label='Baseline')
        
        p.scatter(sample_data['occupancy_rate'] * 100, sample_data['demand_price'],
                  size=8, alpha=0.6, color=self.model_colors['demand'],
                  legend_label='Demand-Based')
        
        p.scatter(sample_data['occupancy_rate'] * 100, sample_data['competitive_price'],
                  size=8, alpha=0.6, color=self.model_colors['competitive'],
                  legend_label='Competitive')
        
        p.legend.location = "top_left"
        return p
    
    def create_price_distribution_plot(self, width=800, height=400):
        """Create histogram of price distributions"""
        p = figure(
            title="Price Distribution Comparison",
            x_axis_label='Price ($)',
            y_axis_label='Frequency',
            width=width,
            height=height,
            tools="pan,wheel_zoom,box_zoom,reset,save"
        )
        
        # Calculate histograms
        hist_baseline, edges_baseline = np.histogram(self.results_df['baseline_price'], bins=30)
        hist_demand, edges_demand = np.histogram(self.results_df['demand_price'], bins=30)
        hist_competitive, edges_competitive = np.histogram(self.results_df['competitive_price'], bins=30)
        
        # Create bar plots
        p.quad(top=hist_baseline, bottom=0, left=edges_baseline[:-1], right=edges_baseline[1:],
               fill_color=self.model_colors['baseline'], alpha=0.6, legend_label='Baseline')
        
        p.quad(top=hist_demand, bottom=0, left=edges_demand[:-1], right=edges_demand[1:],
               fill_color=self.model_colors['demand'], alpha=0.6, legend_label='Demand-Based')
        
        p.quad(top=hist_competitive, bottom=0, left=edges_competitive[:-1], right=edges_competitive[1:],
               fill_color=self.model_colors['competitive'], alpha=0.6, legend_label='Competitive')
        
        p.legend.location = "top_right"
        return p
    
    def create_performance_metrics_table(self):
        """Create performance metrics table"""
        metrics_data = []
        
        for model in ['baseline', 'demand', 'competitive']:
            col = f'{model}_price'
            metrics_data.append({
                'Model': model.title(),
                'Mean Price': f"${self.results_df[col].mean():.2f}",
                'Std Dev': f"${self.results_df[col].std():.2f}",
                'Min Price': f"${self.results_df[col].min():.2f}",
                'Max Price': f"${self.results_df[col].max():.2f}",
                'Price Range': f"${self.results_df[col].max() - self.results_df[col].min():.2f}"
            })
        
        source = ColumnDataSource(pd.DataFrame(metrics_data))
        
        columns = [
            TableColumn(field="Model", title="Model"),
            TableColumn(field="Mean Price", title="Mean Price"),
            TableColumn(field="Std Dev", title="Std Dev"),
            TableColumn(field="Min Price", title="Min Price"),
            TableColumn(field="Max Price", title="Max Price"),
            TableColumn(field="Price Range", title="Price Range"),
        ]
        
        data_table = DataTable(source=source, columns=columns, width=800, height=150)
        return data_table
    
    def create_real_time_dashboard(self):
        """Create complete real-time dashboard"""
        # Create title
        title = Div(text="<h1>Dynamic Parking Pricing Dashboard</h1>", width=1200)
        
        # Create plots
        time_series_plot = self.create_time_series_plot()
        space_wise_plot = self.create_space_wise_plot()
        scatter_plot = self.create_occupancy_price_scatter()
        distribution_plot = self.create_price_distribution_plot()
        metrics_table = self.create_performance_metrics_table()
        
        # Create layout
        dashboard = column(
            title,
            time_series_plot,
            row(space_wise_plot, scatter_plot),
            distribution_plot,
            metrics_table
        )
        
        return dashboard
    
    def show_dashboard(self):
        """Display the complete dashboard"""
        dashboard = self.create_real_time_dashboard()
        show(dashboard)
        return dashboard
    
    def save_dashboard(self, filename="parking_dashboard.html"):
        """Save dashboard to HTML file"""
        from bokeh.io import output_file
        output_file(filename)
        dashboard = self.create_real_time_dashboard()
        show(dashboard)
        print(f"Dashboard saved to {filename}")
