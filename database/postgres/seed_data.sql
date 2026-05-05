-- ============================================
-- SEED DATA GENERATION FOR TESTING
-- ============================================

-- This generates realistic test data for development

-- Generate 10,000 users
INSERT INTO users (email, first_name, last_name, registration_date, country, city, state, zip_code, age_group, gender)
SELECT
    'user' || generate_series || '@example.com',
    'FirstName' || generate_series,
    'LastName' || generate_series,
    CURRENT_DATE - (random() * 730)::integer, -- Random date within last 2 years
    (ARRAY['USA', 'Canada', 'UK', 'Germany', 'France'])[floor(random() * 5 + 1)],
    'City' || (random() * 100)::integer,
    (ARRAY['CA', 'NY', 'TX', 'FL', 'WA'])[floor(random() * 5 + 1)],
    (10000 + random() * 90000)::integer::text,
    (ARRAY['18-24', '25-34', '35-44', '45-54', '55+'])[floor(random() * 5 + 1)],
    (ARRAY['Male', 'Female', 'Other'])[floor(random() * 3 + 1)]
FROM generate_series(1, 10000);

-- Generate 1,000 products
INSERT INTO products (product_name, category, subcategory, brand, unit_cost, current_price, stock_quantity)
SELECT
    'Product ' || generate_series,
    (ARRAY['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books'])[floor(random() * 5 + 1)],
    'Subcategory ' || (random() * 20)::integer,
    'Brand ' || (random() * 50)::integer,
    (random() * 100)::numeric(10,2),
    (random() * 200 + 10)::numeric(10,2),
    (random() * 1000)::integer
FROM generate_series(1, 1000);

-- Generate 100,000 transactions
INSERT INTO transactions (user_id, product_id, transaction_date, quantity, unit_price, total_amount, discount_amount, payment_method)
SELECT
    (random() * 9999 + 1)::integer,
    (random() * 999 + 1)::integer,
    CURRENT_TIMESTAMP - (random() * 180 ||' days')::interval,
    (random() * 5 + 1)::integer,
    (random() * 200 + 10)::numeric(10,2),
    ((random() * 200 + 10) * (random() * 5 + 1))::numeric(12,2),
    (random() * 20)::numeric(10,2),
    (ARRAY['credit_card', 'paypal', 'debit_card', 'apple_pay'])[floor(random() * 4 + 1)]
FROM generate_series(1, 100000);

-- Refresh materialized views
REFRESH MATERIALIZED VIEW daily_metrics;
REFRESH MATERIALIZED VIEW user_metrics;

-- Analyze tables for query optimization
ANALYZE users;
ANALYZE products;
ANALYZE transactions;
