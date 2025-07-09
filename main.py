import pandas as pd
import numpy as np
import pathway as pw
from datetime import datetime
import time
import sys
import os

from data_processor import DataProcessor
from models.baseline_model import BaselineLinearModel
from models.demand_model import DemandBasedModel
from models.competitive_model import CompetitivePricingModel

class ParkingPricingSystem:
    """Main system with enhanced error handling for actual dataset"""
    
    def __init__(self, csv_path="dataset.csv"):
        self.csv_path = csv_path
        self.base_price = 10.0

        self.data_processor = DataProcessor(csv_path)
        
        self.models = {
            'baseline': BaselineLinearModel(base_price=self.base_price),
            'demand': DemandBasedModel(base_price=self.base_price),
            'competitive': CompetitivePricingModel(base_price=self.base_price)
        }

        self.dataset = None
        self.load_and_prepare_dataset()
        
        if self.dataset is not None:
            self.initialize_models()
        else:
            print("Error: Cannot proceed without valid dataset")
            sys.exit(1)
    
    def load_and_prepare_dataset(self):
        """Load and prepare dataset with error handling"""
        print("Loading and preparing dataset...")

        self.dataset = self.data_processor.load_dataset()
        if self.dataset is None:
            return
        
        print(f"\nOriginal dataset shape: {self.dataset.shape}")
        print(f"Original columns: {list(self.dataset.columns)}")

        self.dataset = self.data_processor.preprocess_data(self.dataset)
        if self.dataset is None:
            return
        
        if not self.data_processor.validate_dataset(self.dataset):
            print("Dataset validation failed")
            self.dataset = None
            return
        
        print(f"✓ Dataset ready: {len(self.dataset)} records")
        print(f"✓ Final columns: {list(self.dataset.columns)}")

        print("\nSample of processed data:")
        print(self.dataset[['timestamp', 'space_id', 'occupancy_rate', 'vehicle_type', 'traffic_level']].head())
    
    def initialize_models(self):
        """Initialize pricing models"""
        print("\nInitializing pricing models...")

        space_ids = sorted(self.dataset['space_id'].unique())
        locations_dict = {}

        for space_id in space_ids:
            space_data = self.dataset[self.dataset['space_id'] == space_id].iloc[0]
            locations_dict[space_id] = {
                'latitude': space_data['latitude'],
                'longitude': space_data['longitude']
            }

        self.models['baseline'].initialize_prices(space_ids)
        self.models['demand'].initialize_prices(space_ids)
        self.models['competitive'].initialize_prices(space_ids, locations_dict)

        print(f"✓ Models initialized for {len(space_ids)} parking spaces")
    
    def run_demo_simulation(self):
        """Run pricing simulation"""
        print("\n" + "="*60)
        print("RUNNING DYNAMIC PRICING SIMULATION")
        print("="*60)

        if self.dataset is None:
            print("Error: No dataset available for simulation")
            return
        
        sample_size = min(500, len(self.dataset))
        sample_data = self.dataset.head(sample_size)
        results = []

        print(f"Processing {len(sample_data)} records...")

        for idx, row in sample_data.iterrows():
            try:
                space_id = int(row['space_id'])
                occupancy_rate = float(row['occupancy_rate'])
                queue_length = int(row['queue_length'])
                traffic_level = int(row['traffic_level'])
                is_special_day = bool(row['is_special_day'])
                vehicle_weight = float(row['vehicle_weight'])
                occupancy = int(row['occupancy'])
                capacity = int(row['capacity'])
                timestamp = row['timestamp']

                baseline_price = self.models['baseline'].update_price(
                    space_id, occupancy_rate, timestamp
                )
                
                demand_price = self.models['demand'].update_price(
                    space_id, occupancy_rate, queue_length, traffic_level,
                    is_special_day, vehicle_weight, timestamp
                )
                
                competitive_price = self.models['competitive'].update_price(
                    space_id, occupancy_rate, queue_length, traffic_level,
                    is_special_day, vehicle_weight, occupancy, capacity, timestamp
                )

                results.append({
                    'timestamp': timestamp,
                    'space_id': space_id,
                    'occupancy_rate': occupancy_rate,
                    'queue_length': queue_length,
                    'traffic_level': traffic_level,
                    'is_special_day': is_special_day,
                    'baseline_price': baseline_price,
                    'demand_price': demand_price,
                    'competitive_price': competitive_price
                })

                if idx % 100 == 0:
                    print(f"Processed {idx} records. Space {space_id}: "
                          f"Baseline=${baseline_price:.2f}, "
                          f"Demand=${demand_price:.2f}, "
                          f"Competitive=${competitive_price:.2f}")

            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                continue

        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv('pricing_results.csv', index=False)

            print("\n✓ Demo simulation completed successfully")
            print("✓ Results saved to: pricing_results.csv")
            self.display_summary(results_df)
            return results_df
        else:
            print("No results generated")
            return None
    
    def create_and_show_visualizations(self, results_df):
        """Create and show Bokeh visualizations"""
        print("\n" + "="*60)
        print("CREATING BOKEH VISUALIZATIONS")
        print("="*60)

        try:
            from bokeh.io import output_notebook, output_file
            from visualization.bokeh_dashboard import ParkingVisualizationDashboard

            dashboard = ParkingVisualizationDashboard(results_df)

            print("Choose output option:")
            print("1. Show in browser (HTML file)")
            print("2. Show in Jupyter notebook")
            print("3. Save to file only")

            choice = input("Enter choice (1-3): ").strip()

            if choice == "1":
                output_file("parking_dashboard.html")
                dashboard.show_dashboard()
                print("✓ Dashboard displayed in browser and saved")
            elif choice == "2":
                output_notebook()
                dashboard.show_dashboard()
                print("✓ Dashboard displayed in notebook")
            elif choice == "3":
                dashboard.save_dashboard("parking_dashboard.html")
                print("✓ Dashboard saved to file")
            else:
                output_file("parking_dashboard.html")
                dashboard.show_dashboard()
                print("✓ Dashboard displayed in browser (default)")

            return dashboard

        except ImportError:
            print("Error: Bokeh not installed. Install with: pip install bokeh>=3.0.0")
            return None
        except Exception as e:
            print(f"Error creating visualization: {e}")
            return None
    
    def display_summary(self, results_df):
        """Display summary of simulation results"""
        print("\n" + "="*60)
        print("DYNAMIC PRICING SYSTEM SUMMARY")
        print("="*60)

        print(f"Total records processed: {len(results_df)}")
        print(f"Unique parking spaces: {results_df['space_id'].nunique()}")
        print(f"Time range: {results_df['timestamp'].min()} to {results_df['timestamp'].max()}")

        print(f"\nMODEL PERFORMANCE COMPARISON:")
        print(f"{'Model':<15} {'Avg Price':<12} {'Min Price':<12} {'Max Price':<12} {'Std Dev':<12}")
        print("-" * 60)

        for model in ['baseline', 'demand', 'competitive']:
            col = f'{model}_price'
            print(f"{model.title():<15} ${results_df[col].mean():<11.2f} "
                  f"${results_df[col].min():<11.2f} ${results_df[col].max():<11.2f} "
                  f"${results_df[col].std():<11.2f}")

        print(f"\nPRICE BOUNDS VALIDATION:")
        min_allowed = self.base_price * 0.5
        max_allowed = self.base_price * 2.0

        for model in ['baseline', 'demand', 'competitive']:
            col = f'{model}_price'
            within_bounds = ((results_df[col] >= min_allowed) & (results_df[col] <= max_allowed)).all()
            violations = ((results_df[col] < min_allowed) | (results_df[col] > max_allowed)).sum()
            print(f"{model.title()} Model: {'✓' if within_bounds else '✗'} Within bounds "
                  f"[{min_allowed:.2f}, {max_allowed:.2f}] ({violations} violations)")

        print(f"\nSPACE-WISE ANALYSIS:")
        print(f"{'Space ID':<10} {'Avg Occupancy':<15} {'Avg Baseline':<15} "
              f"{'Avg Demand':<15} {'Avg Competitive':<15}")
        print("-" * 70)

        for space_id in sorted(results_df['space_id'].unique()):
            space_data = results_df[results_df['space_id'] == space_id]
            print(f"{space_id:<10} {space_data['occupancy_rate'].mean():<15.2%} "
                  f"${space_data['baseline_price'].mean():<14.2f} "
                  f"${space_data['demand_price'].mean():<14.2f} "
                  f"${space_data['competitive_price'].mean():<14.2f}")
    
    def run_complete_system_with_viz(self):
        """Run system and optionally create visualizations"""
        print("DYNAMIC PRICING FOR URBAN PARKING LOTS")
        print("="*50)
        print("Capstone Project - Summer Analytics 2025")
        print("="*50)

        print("\nMODEL INFORMATION:")
        for name, model in self.models.items():
            info = model.get_model_info()
            print(f"\n{info['model_name']}:")
            print(f"  Description: {info['description']}")
            print(f"  Base Price: ${info['base_price']}")

        print("\nRunning simulation...")
        results = self.run_demo_simulation()

        if results is not None:
            viz_choice = input("\nWould you like to create visualizations? (y/n): ").strip().lower()
            if viz_choice in ['y', 'yes']:
                dashboard = self.create_and_show_visualizations(results)
                if dashboard:
                    print("\nDashboard created with pricing analysis and visual comparisons")

            print("\nResults saved to 'pricing_results.csv'")
        else:
            print("\nSimulation failed")
        
        return results

if __name__ == "__main__":
    print("DYNAMIC PRICING FOR URBAN PARKING LOTS")
    print("="*50)
    print("Capstone Project - Summer Analytics 2025")
    print("="*50)

    try:
        system = ParkingPricingSystem("dataset.csv")
        results = system.run_complete_system_with_viz()

        if results is not None:
            print("\nSystem executed successfully. Check 'pricing_results.csv' for details.")
        else:
            print("\nSystem completed with issues")
    except Exception as e:
        print(f"\nSystem failed with error: {e}")
        import traceback
        traceback.print_exc()
