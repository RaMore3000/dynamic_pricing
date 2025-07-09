import numpy as np
import pandas as pd

class BaselineLinearModel:
    """Baseline Linear Model: adjusts price linearly based on occupancy rate"""

    def __init__(self, base_price=10.0, alpha=0.5):
        self.base_price = base_price
        self.alpha = alpha
        self.current_prices = {}
        self.price_history = {}

    def initialize_prices(self, space_ids):
        """Initialize price and history for each parking space"""
        for space_id in space_ids:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]

    def update_price(self, space_id, occupancy_rate, timestamp=None):
        """Update price using: Price(t+1) = Price(t) + α * occupancy_rate"""
        if space_id not in self.current_prices:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]

        current_price = self.current_prices[space_id]
        price_change = self.alpha * occupancy_rate
        new_price = current_price + price_change

        min_price = self.base_price * 0.5
        max_price = self.base_price * 2.0

        if new_price < min_price:
            new_price = min_price + (new_price - min_price) * 0.1
        elif new_price > max_price:
            new_price = max_price - (new_price - max_price) * 0.1

        new_price = max(min_price, min(new_price, max_price))

        self.current_prices[space_id] = new_price
        self.price_history[space_id].append(new_price)

        return new_price

    def get_current_price(self, space_id):
        """Return current price for given space"""
        return self.current_prices.get(space_id, self.base_price)

    def get_price_history(self, space_id):
        """Return price history for given space"""
        return self.price_history.get(space_id, [self.base_price])

    def get_model_info(self):
        """Return model metadata"""
        return {
            'model_name': 'Baseline Linear Model',
            'base_price': self.base_price,
            'alpha': self.alpha,
            'description': 'Simple linear price adjustment based on occupancy rate'
        }