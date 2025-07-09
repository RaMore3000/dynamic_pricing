import pathway as pw
import pandas as pd
import numpy as np

class PathwayStreamProcessor:
    """Handles real-time streaming and processing using Pathway"""

    def __init__(self, csv_path="dataset.csv"):
        self.csv_path = csv_path

    def create_streaming_table(self, input_rate=50):
        """Create streaming table from CSV with defined schema"""
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

        return pw.demo.replay_csv(
            self.csv_path,
            schema=ParkingSchema,
            input_rate=input_rate,
            timestamp_column="timestamp"
        )

    def add_derived_features(self, table):
        """Add computed features like occupancy rate and time-based fields"""
        table = table.with_columns(
            occupancy_rate=pw.this.occupancy / pw.this.capacity,
            hour=pw.this.timestamp.dt.hour,
            day_of_week=pw.this.timestamp.dt.dayofweek
        )

        vehicle_weights = {'car': 1.0, 'bike': 0.5, 'truck': 1.5}
        table = table.with_columns(
            vehicle_weight=pw.this.vehicle_type.map(vehicle_weights)
        )

        return table

    def create_pricing_processor(self, table, models_dict):
        """Attach model-based pricing logic via UDF"""

        @pw.udf
        def calculate_all_prices(
            space_id: int, occupancy: int, capacity: int, queue_length: int,
            traffic_level: int, is_special_day: bool, vehicle_type: str,
            occupancy_rate: float, vehicle_weight: float
        ) -> pw.Json:
            results = {}

            if 'baseline' in models_dict:
                results['baseline_price'] = models_dict['baseline'].update_price(
                    space_id, occupancy_rate
                )

            if 'demand' in models_dict:
                results['demand_price'] = models_dict['demand'].update_price(
                    space_id, occupancy_rate, queue_length, traffic_level,
                    is_special_day, vehicle_weight
                )

            if 'competitive' in models_dict:
                results['competitive_price'] = models_dict['competitive'].update_price(
                    space_id, occupancy_rate, queue_length, traffic_level,
                    is_special_day, vehicle_weight, occupancy, capacity
                )

            return results

        return table.select(
            pw.this.timestamp,
            pw.this.space_id,
            pw.this.occupancy,
            pw.this.capacity,
            pw.this.occupancy_rate,
            pw.this.queue_length,
            pw.this.traffic_level,
            pw.this.is_special_day,
            pw.this.vehicle_type,
            pw.this.vehicle_weight,
            pricing_results=calculate_all_prices(
                pw.this.space_id, pw.this.occupancy, pw.this.capacity,
                pw.this.queue_length, pw.this.traffic_level, pw.this.is_special_day,
                pw.this.vehicle_type, pw.this.occupancy_rate, pw.this.vehicle_weight
            )
        )

    def setup_output_sink(self, table, output_path="pricing_results.jsonl"):
        """Write results to JSONL output"""
        pw.io.jsonlines.write(table, output_path)
