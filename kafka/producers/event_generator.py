"""
Real-time E-Commerce Event Generator
Simulates realistic user behavior: page views, clicks, add-to-cart, purchases
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import psycopg2


class EventGenerator:
    def __init__(self):
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # PostgreSQL connection (to get real user/product IDs)
        self.conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='ecommerce',
            user='postgres',
            password='postgres'
        )

        # Cache user and product IDs
        self.user_ids = self._get_user_ids()
        self.product_ids = self._get_product_ids()

        print(f"Loaded {len(self.user_ids)} users and {len(self.product_ids)} products")

    def _get_user_ids(self):
        """Fetch all user IDs from database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT user_id FROM users LIMIT 1000;")
        return [row[0] for row in cursor.fetchall()]

    def _get_product_ids(self):
        """Fetch all product IDs from database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_id FROM products;")
        return [row[0] for row in cursor.fetchall()]

    def generate_session_id(self):
        """Generate unique session ID"""
        return f"session_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

    def generate_page_view_event(self, user_id, session_id):
        """Generate a page view event"""
        pages = [
            '/home', '/products', '/product-detail', '/cart',
            '/checkout', '/account', '/search', '/category'
        ]

        event = {
            'event_id': f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            'event_type': 'page_view',
            'user_id': user_id,
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'page': random.choice(pages),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'referrer': random.choice(['google', 'facebook', 'direct', 'email'])
        }
        return event

    def generate_product_click_event(self, user_id, session_id):
        """Generate a product click event"""
        event = {
            'event_id': f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            'event_type': 'product_click',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': random.choice(self.product_ids),
            'timestamp': datetime.now().isoformat(),
            'position': random.randint(1, 20),
            'source': random.choice(['search', 'recommendation', 'category', 'homepage'])
        }
        return event

    def generate_add_to_cart_event(self, user_id, session_id):
        """Generate add-to-cart event"""
        event = {
            'event_id': f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            'event_type': 'add_to_cart',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': random.choice(self.product_ids),
            'quantity': random.randint(1, 3),
            'timestamp': datetime.now().isoformat()
        }
        return event

    def generate_purchase_event(self, user_id, session_id):
        """Generate purchase event"""
        product_id = random.choice(self.product_ids)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)

        event = {
            'event_id': f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            'event_type': 'purchase',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': round(quantity * unit_price, 2),
            'payment_method': random.choice(['credit_card', 'paypal', 'debit_card', 'apple_pay']),
            'timestamp': datetime.now().isoformat()
        }
        return event

    def simulate_user_session(self):
        """Simulate a realistic user session"""
        user_id = random.choice(self.user_ids)
        session_id = self.generate_session_id()

        # User journey probabilities
        # 100% page view
        # 70% product click
        # 40% add to cart
        # 20% purchase

        # Page view (always)
        event = self.generate_page_view_event(user_id, session_id)
        self.producer.send('user_events', event)

        # Product click (70% chance)
        if random.random() < 0.7:
            time.sleep(random.uniform(0.1, 0.5))
            event = self.generate_product_click_event(user_id, session_id)
            self.producer.send('user_events', event)

            # Add to cart (40% chance after clicking)
            if random.random() < 0.4:
                time.sleep(random.uniform(0.2, 1.0))
                event = self.generate_add_to_cart_event(user_id, session_id)
                self.producer.send('user_events', event)

                # Purchase (20% chance after adding to cart)
                if random.random() < 0.2:
                    time.sleep(random.uniform(0.5, 2.0))
                    event = self.generate_purchase_event(user_id, session_id)
                    self.producer.send('transactions', event)

    def run(self, events_per_second=100, duration_seconds=None):
        """
        Run the event generator

        Args:
            events_per_second: Target event rate (approximate)
            duration_seconds: How long to run (None = infinite)
        """
        print(f"\nStarting event generator...")
        print(f"   Target rate: ~{events_per_second} events/sec")
        print(f"   Duration: {'infinite' if duration_seconds is None else f'{duration_seconds}s'}")
        print(f"   Topics: user_events, transactions")
        print(f"\nPress Ctrl+C to stop\n")

        start_time = time.time()
        event_count = 0

        try:
            while True:
                # Check duration
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break

                # Generate session
                self.simulate_user_session()
                event_count += 1

                # Print stats every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    print(f"[STATS] Events: {event_count} | Rate: {rate:.1f}/sec | Elapsed: {elapsed:.1f}s")

                # Sleep to control rate (approximate)
                time.sleep(1.0 / events_per_second)

        except KeyboardInterrupt:
            print("\n\nStopping event generator...")

        finally:
            elapsed = time.time() - start_time
            rate = event_count / elapsed if elapsed > 0 else 0
            print(f"\nFinal Stats:")
            print(f"   Total events: {event_count}")
            print(f"   Duration: {elapsed:.1f}s")
            print(f"   Average rate: {rate:.1f} events/sec")

            self.producer.flush()
            self.producer.close()
            self.conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='E-Commerce Event Generator')
    parser.add_argument('--rate', type=int, default=100, help='Events per second (default: 100)')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds (default: infinite)')

    args = parser.parse_args()

    generator = EventGenerator()
    generator.run(events_per_second=args.rate, duration_seconds=args.duration)
