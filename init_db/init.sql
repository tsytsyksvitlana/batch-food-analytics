CREATE TABLE IF NOT EXISTS pizza_categories (
    pizza_name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL
);


INSERT INTO pizza_categories (pizza_name, category)
VALUES
('cali_ckn', 'chicken'),
('margherita', 'vegetarian'),
('pepperoni', 'meat'),
('veggie', 'vegetarian')
ON CONFLICT DO NOTHING;