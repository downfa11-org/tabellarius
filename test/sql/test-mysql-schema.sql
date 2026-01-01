CREATE TABLE IF NOT EXISTS orders (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(64) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1️. INSERT (users)
INSERT INTO users (email, name, created_at) VALUES
('user11@example.com', 'User 11', NOW()),
('user12@example.com', 'User 12', NOW() + INTERVAL 5 SECOND);

-- 2️. UPDATE (orders)
UPDATE orders
SET status = 'completed'
WHERE id = 1;

-- 3️. DELETE (users)
DELETE FROM users
WHERE id = 2;

-- 4️. Transaction (orders)
START TRANSACTION;

INSERT INTO orders (user_id, amount, status, created_at) VALUES
(3, 300.00, 'pending', NOW()),
(4, 150.50, 'pending', NOW() + INTERVAL 2 SECOND),
(5, 220.25, 'pending', NOW() + INTERVAL 4 SECOND);

COMMIT;

-- 5️. rollback (orders, not commited)
START TRANSACTION;

INSERT INTO orders (user_id, amount, status, created_at) VALUES
(6, 500.00, 'pending', NOW()),
(7, 450.50, 'pending', NOW() + INTERVAL 2 SECOND);

ROLLBACK;

-- 6️. transaction (orders, users)
START TRANSACTION;

UPDATE orders
SET status = 'completed'
WHERE id = 3;

UPDATE users
SET name = 'Updated User 3'
WHERE id = 3;

COMMIT;

-- 7. ddl alter (orders)
ALTER TABLE orders ADD COLUMN note VARCHAR(255) DEFAULT NULL;

-- 8️. ddl delete
DROP TABLE IF EXISTS temp_table_for_cdc_test;