-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Generates 50,000 rows in inventory.customers, with 5 ADD COLUMN and 4
-- MODIFY COLUMN operations in the middle so CDC pipelines can observe
-- CreateTable / AddColumn / AlterColumnType events.

CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    ID    BIGINT       NOT NULL,
    name  VARCHAR(32)  NULL,
    email VARCHAR(64)  NULL,
    age   INT          NULL,
    PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET SESSION cte_max_recursion_depth = 100000;

-- Stage 1: 20,000 rows with the initial schema.
INSERT INTO customers (ID, name, email, age)
WITH RECURSIVE seq AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 20000
)
SELECT
    n,
    CONCAT('user_', n),
    CONCAT('user_', n, '@example.com'),
    n % 120
FROM seq;

-- Stage 2: add 5 columns.
ALTER TABLE customers
    ADD COLUMN phone   VARCHAR(16)   NULL AFTER age,
    ADD COLUMN score   INT           NULL AFTER phone,
    ADD COLUMN balance DECIMAL(10,2) NULL AFTER score,
    ADD COLUMN active  TINYINT       NULL AFTER balance,
    ADD COLUMN extra   VARCHAR(32)   NULL AFTER active;

-- Stage 3: modify 4 column types.
ALTER TABLE customers
    MODIFY COLUMN name  VARCHAR(128) NULL,
    MODIFY COLUMN email VARCHAR(256) NULL,
    MODIFY COLUMN age   BIGINT       NULL,
    MODIFY COLUMN score BIGINT       NULL;

-- Stage 4: remaining 30,000 rows with the evolved schema (ID 20001..50000).
INSERT INTO customers (ID, name, email, age, phone, score, balance, active, extra)
WITH RECURSIVE seq AS (
    SELECT 20001 AS n
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 50000
)
SELECT
    n,
    CONCAT('user_', n),
    CONCAT('user_', n, '@example.com'),
    n % 120,
    CONCAT('138', LPAD(n, 8, '0')),
    n % 1000,
    ROUND((n % 10000) / 100, 2),
    n % 2,
    CONCAT('extra_', n)
FROM seq;

SELECT COUNT(*) AS total_rows FROM customers;
SHOW CREATE TABLE customers;
DESC customers;
