import pandas as pd
import numpy as np
from datetime import datetime
import pathway as pw

class DataProcessor:
    def __init__(self, csv_path="dataset.csv"):
        self.csv_path = csv_path
        self.base_price = 10.0
        
    def load_dataset(self):
        """Load dataset from CSV"""
        try:
            df = pd.read_csv(self.csv_path)
            print(f"Dataset loaded successfully: {len(df)} records")
            return df
        except FileNotFoundError:
            print(f"Error: {self.csv_path} not found.")
            return None
        except Exception as e:
            print(f"Error loading dataset: {e}")
            return None
    
    def map_columns(self, df):
        """Map raw dataset columns to expected names"""
        column_mapping = {
            'ID': 'space_id',
            'Capacity': 'capacity',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Occupancy': 'occupancy',
            'VehicleType': 'vehicle_type',
            'TrafficConditionNearby': 'traffic_level',
            'QueueLength': 'queue_length',
            'IsSpecialDay': 'is_special_day',
            'LastUpdatedDate': 'date',
            'LastUpdatedTime': 'time'
        }
        
        missing_columns = [col for col in column_mapping if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            return None
        
        df = df.rename(columns=column_mapping)
        df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['time'], dayfirst=True, errors='coerce')
        df = df.drop(['date', 'time'], axis=1)

        if 'SystemCodeNumber' in df.columns:
            df['system_code'] = df['SystemCodeNumber']
        
        return df
    
    def validate_dataset(self, df):
        """Check presence and integrity of required columns"""
        print("\nValidating dataset...")

        required_columns = [
            'timestamp', 'space_id', 'latitude', 'longitude', 
            'capacity', 'occupancy', 'queue_length', 'vehicle_type',
            'traffic_level', 'is_special_day'
        ]
        
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            print(f"Missing columns: {missing}")
            return False
        
        print(f"Unique parking spaces: {df['space_id'].nunique()}")
        print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"Space ID range: {df['space_id'].min()} to {df['space_id'].max()}")
        print(f"Capacity range: {df['capacity'].min()} to {df['capacity'].max()}")
        print(f"Occupancy range: {df['occupancy'].min()} to {df['occupancy'].max()}")
        
        return True
    
    def preprocess_data(self, df):
        """Preprocess and enrich dataset for model input"""
        try:
            df = self.map_columns(df)
            if df is None:
                return None
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            if df['space_id'].min() == 1:
                df['space_id'] = df['space_id'] - 1
                print("Converted space_id to 0-based indexing")

            df['occupancy_rate'] = df['occupancy'] / df['capacity']
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek

            vehicle_type_mapping = {
                'Car': 'car', 'car': 'car', 'CAR': 'car',
                'Bike': 'bike', 'bike': 'bike', 'BIKE': 'bike',
                'Truck': 'truck', 'truck': 'truck', 'TRUCK': 'truck',
                'Motorcycle': 'bike', 'motorcycle': 'bike',
                'Bus': 'truck', 'bus': 'truck'
            }
            df['vehicle_type'] = df['vehicle_type'].map(vehicle_type_mapping).fillna('car')

            vehicle_weights = {'car': 1.0, 'bike': 0.5, 'truck': 1.5}
            df['vehicle_weight'] = df['vehicle_type'].map(vehicle_weights)

            if df['traffic_level'].dtype == 'object':
                traffic_mapping = {
                    'Low': 1, 'low': 1, 'LOW': 1,
                    'Medium': 5, 'medium': 5, 'MEDIUM': 5,
                    'High': 10, 'high': 10, 'HIGH': 10,
                    'Light': 2, 'light': 2,
                    'Moderate': 6, 'moderate': 6,
                    'Heavy': 9, 'heavy': 9,
                    'Severe': 10, 'severe': 10
                }
                df['traffic_level'] = df['traffic_level'].map(traffic_mapping).fillna(5)

            df['traffic_level'] = np.clip(df['traffic_level'], 1, 10)

            if df['is_special_day'].dtype == 'object':
                df['is_special_day'] = df['is_special_day'].map({
                    'True': True, 'true': True, 'TRUE': True, 'Yes': True, 'yes': True,
                    'False': False, 'false': False, 'FALSE': False, 'No': False, 'no': False
                }).fillna(False)

            print("Data preprocessing completed")
            return df

        except Exception as e:
            print(f"Error in preprocessing: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def setup_pathway_schema(self):
        """Return schema for Pathway streaming"""
        class ParkingSchema(pw.Schema):
            timestamp: str
            space_id: int
            latitude: float
            longitude: float
            capacity: int
            occupancy: int
            queue_length: int
            vehicle_type: str
            traffic_level: int
            is_special_day: bool
            occupancy_rate: float
            hour: int
            day_of_week: int
            vehicle_weight: float

        return ParkingSchema