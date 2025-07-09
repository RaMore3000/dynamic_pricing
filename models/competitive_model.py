import numpy as np
import pandas as pd

class CompetitivePricingModel:
    """Competitive Pricing Model with location and demand adjustments"""

    def __init__(self, base_price=10.0):
        self.base_price = base_price
        self.current_prices = {}
        self.price_history = {}
        self.locations = {}

        # Demand model parameters
        self.alpha = 0.6
        self.beta = 0.3
        self.gamma = 0.2
        self.delta = 0.4
        self.epsilon = 0.1
        self.lambda_param = 0.8

        # Competition parameters
        self.competition_radius = 0.005
        self.competition_weight = 0.3
        self.demand_weight = 0.7

    def initialize_prices(self, space_ids, locations_dict):
        """Initialize prices and location data"""
        for space_id in space_ids:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]
        self.locations = locations_dict

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Euclidean distance (approximate)"""
        return np.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)

    def find_competitors(self, space_id):
        """Find nearby competitors within a fixed radius"""
        if space_id not in self.locations:
            return []

        current_location = self.locations[space_id]
        competitors = []

        for other_space_id, other_location in self.locations.items():
            if other_space_id == space_id:
                continue
            distance = self.calculate_distance(
                current_location['latitude'], current_location['longitude'],
                other_location['latitude'], other_location['longitude']
            )
            if distance <= self.competition_radius:
                competitors.append({
                    'space_id': other_space_id,
                    'distance': distance,
                    'price': self.current_prices.get(other_space_id, self.base_price)
                })

        return sorted(competitors, key=lambda x: x['distance'])

    def calculate_demand(self, occupancy_rate, queue_length, traffic_level, is_special_day, vehicle_weight):
        """Demand estimation using a weighted formula"""
        normalized_queue = min(queue_length / 20.0, 1.0)
        normalized_traffic = (traffic_level - 1) / 9.0
        special_day_factor = 1.0 if is_special_day else 0.0

        demand = (
            self.alpha * occupancy_rate +
            self.beta * normalized_queue -
            self.gamma * normalized_traffic +
            self.delta * special_day_factor +
            self.epsilon * vehicle_weight
        )
        return np.tanh(demand)

    def calculate_competitive_adjustment(self, space_id, occupancy, capacity):
        """Adjustment based on nearby competitor pricing"""
        competitors = self.find_competitors(space_id)
        if not competitors:
            return 0.0

        avg_price = np.mean([comp['price'] for comp in competitors])
        current_price = self.current_prices.get(space_id, self.base_price)

        if occupancy >= capacity:
            return -0.1 if avg_price < current_price else 0.05

        price_ratio = avg_price / current_price
        return (price_ratio - 1) * 0.2

    def update_price(self, space_id, occupancy_rate, queue_length, traffic_level,
                     is_special_day, vehicle_weight, occupancy, capacity, timestamp=None):
        """Update price using demand and competition"""
        if space_id not in self.current_prices:
            self.current_prices[space_id] = self.base_price
            self.price_history[space_id] = [self.base_price]

        demand = self.calculate_demand(
            occupancy_rate, queue_length, traffic_level, is_special_day, vehicle_weight
        )
        demand_price = self.base_price * (1 + self.lambda_param * demand)

        adjustment = self.calculate_competitive_adjustment(space_id, occupancy, capacity)
        competitive_price = self.current_prices[space_id] * (1 + adjustment)

        final_price = (
            self.demand_weight * demand_price +
            self.competition_weight * competitive_price
        )

        final_price = max(self.base_price * 0.5, min(final_price, self.base_price * 2.0))

        self.current_prices[space_id] = final_price
        self.price_history[space_id].append(final_price)

        return final_price

    def suggest_rerouting(self, space_id, occupancy, capacity):
        """Suggest cheaper nearby options when full"""
        if occupancy < capacity:
            return []

        competitors = self.find_competitors(space_id)
        current_price = self.current_prices.get(space_id, self.base_price)

        suggestions = []
        for comp in competitors:
            if comp['price'] <= current_price * 1.1:
                suggestions.append({
                    'space_id': comp['space_id'],
                    'distance_km': comp['distance'] * 111,
                    'price': comp['price'],
                    'savings': current_price - comp['price']
                })

        return suggestions[:3]

    def get_current_price(self, space_id):
        """Return current price"""
        return self.current_prices.get(space_id, self.base_price)

    def get_price_history(self, space_id):
        """Return price history"""
        return self.price_history.get(space_id, [self.base_price])

    def get_model_info(self):
        """Return model details"""
        return {
            'model_name': 'Competitive Pricing Model',
            'base_price': self.base_price,
            'competition_radius_km': self.competition_radius * 111,
            'weights': {
                'demand_weight': self.demand_weight,
                'competition_weight': self.competition_weight
            },
            'description': 'Advanced pricing with location intelligence and competition analysis'
        }
