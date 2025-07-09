import numpy as np
import pandas as pd

class DemandBasedModel:
    """Demand-Based Pricing Model using occupancy, queue, traffic, and other factors"""

    def __init__(self, base_price=10.0):
        self.base_price = base_price
        self.current_prices = {}
        self.price_history = {}

        # Demand function parameters
        self.alpha = 0.6
        self.beta = 0.3
        self.gamma = 0.2
        self.delta = 0.4
        self.epsilon = 0.1
        self.lambda_param = 0.8

    def initialize_prices(self, space_ids):
        """Initialize prices for all spaces"""
        for space_id in space_ids:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]

    def calculate_demand(self, occupancy_rate, queue_length, traffic_level, is_special_day, vehicle_weight):
        """Calculate raw demand score based on weighted features"""
        normalized_queue = min(queue_length / 20.0, 1.0)
        normalized_traffic = (traffic_level - 1) / 9.0
        special_day_factor = 1.0 if is_special_day else 0.0

        return (
            self.alpha * occupancy_rate +
            self.beta * normalized_queue -
            self.gamma * normalized_traffic +
            self.delta * special_day_factor +
            self.epsilon * vehicle_weight
        )

    def normalize_demand(self, demand):
        """Smooth demand using tanh"""
        return np.tanh(demand)

    def update_price(self, space_id, occupancy_rate, queue_length, traffic_level,
                     is_special_day, vehicle_weight, timestamp=None):
        """Update price based on current demand"""
        if space_id not in self.current_prices:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]

        demand = self.calculate_demand(occupancy_rate, queue_length, traffic_level, is_special_day, vehicle_weight)
        normalized_demand = self.normalize_demand(demand)
        new_price = self.base_price * (1 + self.lambda_param * normalized_demand)

        new_price = max(self.base_price * 0.5, min(new_price, self.base_price * 2.0))

        self.current_prices[space_id] = new_price
        self.price_history[space_id].append(new_price)

        return new_price

    def get_current_price(self, space_id):
        """Return current price for a space"""
        return self.current_prices.get(space_id, self.base_price)

    def get_price_history(self, space_id):
        """Return price history for a space"""
        return self.price_history.get(space_id, [self.base_price])

    def get_model_info(self):
        """Return model parameters and metadata"""
        return {
            'model_name': 'Demand-Based Price Function',
            'base_price': self.base_price,
            'parameters': {
                'alpha': self.alpha,
                'beta': self.beta,
                'gamma': self.gamma,
                'delta': self.delta,
                'epsilon': self.epsilon,
                'lambda': self.lambda_param
            },
            'description': 'Advanced pricing using multiple demand factors'
        }
