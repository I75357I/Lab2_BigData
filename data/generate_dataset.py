"""
Dataset generator for Lab2: Big Data with Hadoop & Spark
Generates 120 000 rows of e-commerce transactions with 11 features:
  - transaction_id   : int
  - date             : string (date)
  - category         : string (categorical – 6 values)
  - product_name     : string (categorical – 36 unique values)
  - quantity         : int
  - unit_price       : float
  - discount_pct     : int
  - total_amount     : float
  - region           : string (categorical – 5 values)
  - customer_age     : int
  - payment_method   : string (categorical – 5 values)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
N = 120_000

CATEGORIES = ['Electronics', 'Clothing', 'Food & Beverages', 'Books', 'Sports & Outdoors', 'Home & Garden']
REGIONS = ['North', 'South', 'East', 'West', 'Central']
PAYMENT_METHODS = ['Cash', 'Credit Card', 'Debit Card', 'Online Transfer', 'Mobile Payment']

PRODUCTS = {
    'Electronics':      ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera'],
    'Clothing':         ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes', 'Hat'],
    'Food & Beverages': ['Coffee', 'Tea', 'Snacks', 'Juice', 'Energy Drink', 'Water'],
    'Books':            ['Novel', 'Textbook', 'Magazine', 'Comic', 'Cookbook', 'Biography'],
    'Sports & Outdoors':['Running Shoes', 'Yoga Mat', 'Bicycle', 'Tent', 'Backpack', 'Weights'],
    'Home & Garden':    ['Plant Pot', 'Tool Set', 'Lamp', 'Cushion', 'Curtains', 'Rug'],
}

PRICE_RANGES = {
    'Electronics':      (50.0,  2000.0),
    'Clothing':         (10.0,   200.0),
    'Food & Beverages': (1.0,     30.0),
    'Books':            (5.0,     80.0),
    'Sports & Outdoors':(15.0,   500.0),
    'Home & Garden':    (5.0,    150.0),
}

start_date = datetime(2022, 1, 1)
dates = [start_date + timedelta(days=int(d)) for d in np.random.randint(0, 730, N)]

categories   = np.random.choice(CATEGORIES, N)
products     = np.array([np.random.choice(PRODUCTS[c]) for c in categories])
unit_prices  = np.array([round(float(np.random.uniform(*PRICE_RANGES[c])), 2) for c in categories])
quantities   = np.random.randint(1, 11, N)
discounts    = np.random.choice([0, 5, 10, 15, 20, 25], N)
totals       = np.round(unit_prices * quantities * (1 - discounts / 100), 2)
customer_ages = np.random.randint(18, 76, N)

df = pd.DataFrame({
    'transaction_id':  range(1, N + 1),
    'date':            [d.strftime('%Y-%m-%d') for d in dates],
    'category':        categories,
    'product_name':    products,
    'quantity':        quantities,
    'unit_price':      unit_prices,
    'discount_pct':    discounts,
    'total_amount':    totals,
    'region':          np.random.choice(REGIONS, N),
    'customer_age':    customer_ages,
    'payment_method':  np.random.choice(PAYMENT_METHODS, N),
})

output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dataset.csv')
df.to_csv(output_path, index=False)

size_mb = os.path.getsize(output_path) / 1024 / 1024
print(f"Dataset generated: {output_path}")
print(f"Rows: {len(df):,}  |  Columns: {len(df.columns)}  |  File size: {size_mb:.2f} MB")
print(f"\nColumn dtypes:\n{df.dtypes}")
print(f"\nFirst 3 rows:\n{df.head(3).to_string()}")
print(f"\nCategory distribution:\n{df['category'].value_counts()}")
