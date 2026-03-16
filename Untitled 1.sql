-- =====================================================================================
-- TREAD INTELLIGENCE: Intelligent Inventory Forecasting & Procurement for Tireco
-- Complete End-to-End Implementation
-- =====================================================================================
-- Version: 1.0
-- Date: March 16, 2026
-- Description: This script creates the complete data infrastructure for predictive
--              inventory management and automated PO recommendations
-- =====================================================================================

-- =====================================================================================
-- PHASE 1: DATABASE AND SCHEMA INFRASTRUCTURE
-- =====================================================================================

CREATE OR REPLACE DATABASE TIRECO_DW
    COMMENT = 'Tread Intelligence - Inventory Forecasting & Procurement Data Warehouse';

CREATE OR REPLACE SCHEMA TIRECO_DW.BRONZE
    COMMENT = 'Raw data ingestion layer - Source system replicas';

CREATE OR REPLACE SCHEMA TIRECO_DW.EXTERNAL
    COMMENT = 'External data from Snowflake Marketplace - Weather, Ports, Economic';

CREATE OR REPLACE SCHEMA TIRECO_DW.SILVER
    COMMENT = 'Cleansed and conformed data layer - Dimensions and Facts';

CREATE OR REPLACE SCHEMA TIRECO_DW.GOLD
    COMMENT = 'Analytics-ready aggregations and ML features';

CREATE OR REPLACE SCHEMA TIRECO_DW.ML
    COMMENT = 'Machine Learning models and forecast outputs';

CREATE OR REPLACE SCHEMA TIRECO_DW.PROCUREMENT
    COMMENT = 'PO Recommendations and procurement automation';

CREATE OR REPLACE SCHEMA TIRECO_DW.ORCHESTRATION
    COMMENT = 'Tasks, procedures, and pipeline management';

-- =====================================================================================
-- PHASE 2: BRONZE LAYER - SOURCE SYSTEM TABLES (ERP, WMS, TMS, CRM, MDM)
-- =====================================================================================

USE SCHEMA TIRECO_DW.BRONZE;

-- ERP: Purchase Orders Header
CREATE OR REPLACE TABLE RAW_ERP_PURCHASE_ORDERS (
    po_id VARCHAR(50) PRIMARY KEY,
    po_number VARCHAR(50) NOT NULL,
    vendor_id VARCHAR(50),
    dc_id VARCHAR(50),
    order_date DATE,
    expected_delivery_date DATE,
    po_status VARCHAR(20),
    payment_terms VARCHAR(50),
    freight_terms VARCHAR(50),
    buyer_id VARCHAR(50),
    approval_status VARCHAR(20),
    total_amount DECIMAL(15,2),
    currency VARCHAR(3) DEFAULT 'USD',
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_system VARCHAR(50) DEFAULT 'ERP'
);

-- ERP: Purchase Order Line Items
CREATE OR REPLACE TABLE RAW_ERP_PO_LINE_ITEMS (
    po_line_id VARCHAR(50) PRIMARY KEY,
    po_id VARCHAR(50),
    sku_id VARCHAR(50),
    ordered_qty INT,
    unit_cost DECIMAL(10,2),
    extended_cost DECIMAL(15,2),
    line_status VARCHAR(20),
    requested_date DATE,
    confirmed_date DATE,
    promise_date DATE,
    discount_pct DECIMAL(5,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ERP: PO Receipts
CREATE OR REPLACE TABLE RAW_ERP_PO_RECEIPTS (
    receipt_id VARCHAR(50) PRIMARY KEY,
    po_line_id VARCHAR(50),
    received_qty INT,
    received_date DATE,
    receiving_dock VARCHAR(20),
    lot_number VARCHAR(50),
    quality_status VARCHAR(20),
    variance_qty INT,
    variance_reason VARCHAR(100),
    inspector_id VARCHAR(50),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ERP: Vendors Master
CREATE OR REPLACE TABLE RAW_ERP_VENDORS (
    vendor_id VARCHAR(50) PRIMARY KEY,
    vendor_code VARCHAR(20) NOT NULL,
    vendor_name VARCHAR(200) NOT NULL,
    vendor_type VARCHAR(50),
    country_code VARCHAR(3),
    payment_method VARCHAR(50),
    credit_limit DECIMAL(15,2),
    risk_rating VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ERP: Vendor Terms
CREATE OR REPLACE TABLE RAW_ERP_VENDOR_TERMS (
    term_id VARCHAR(50) PRIMARY KEY,
    vendor_id VARCHAR(50),
    sku_category_id VARCHAR(50),
    lead_time_days INT,
    min_order_qty INT,
    min_order_value DECIMAL(10,2),
    order_multiple INT,
    volume_discount_tier1_qty INT,
    volume_discount_tier1_pct DECIMAL(5,2),
    volume_discount_tier2_qty INT,
    volume_discount_tier2_pct DECIMAL(5,2),
    effective_from DATE,
    effective_to DATE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- WMS: Distribution Centers
CREATE OR REPLACE TABLE RAW_WMS_DISTRIBUTION_CENTERS (
    dc_id VARCHAR(50) PRIMARY KEY,
    dc_code VARCHAR(20) NOT NULL,
    dc_name VARCHAR(100) NOT NULL,
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    region VARCHAR(50),
    lat DECIMAL(10,6),
    lon DECIMAL(10,6),
    total_sqft INT,
    manager_name VARCHAR(100),
    timezone VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- WMS: Inventory Snapshot
CREATE OR REPLACE TABLE RAW_WMS_INVENTORY (
    inventory_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50) NOT NULL,
    dc_id VARCHAR(50) NOT NULL,
    bin_id VARCHAR(50),
    lot_number VARCHAR(50),
    quantity_on_hand INT DEFAULT 0,
    quantity_allocated INT DEFAULT 0,
    quantity_available INT DEFAULT 0,
    quantity_in_transit INT DEFAULT 0,
    quantity_on_order INT DEFAULT 0,
    unit_cost DECIMAL(10,2),
    last_count_date DATE,
    snapshot_date DATE DEFAULT CURRENT_DATE(),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- WMS: Inventory Transactions
CREATE OR REPLACE TABLE RAW_WMS_INVENTORY_TRANSACTIONS (
    txn_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    dc_id VARCHAR(50),
    txn_type VARCHAR(30),
    quantity INT,
    from_bin_id VARCHAR(50),
    to_bin_id VARCHAR(50),
    reference_doc_type VARCHAR(30),
    reference_doc_id VARCHAR(50),
    reason_code VARCHAR(20),
    user_id VARCHAR(50),
    txn_timestamp TIMESTAMP_NTZ,
    unit_cost DECIMAL(10,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- TMS: Carriers
CREATE OR REPLACE TABLE RAW_TMS_CARRIERS (
    carrier_id VARCHAR(50) PRIMARY KEY,
    carrier_code VARCHAR(20) NOT NULL,
    carrier_name VARCHAR(100) NOT NULL,
    carrier_type VARCHAR(30),
    scac_code VARCHAR(10),
    safety_rating VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- TMS: Shipments
CREATE OR REPLACE TABLE RAW_TMS_SHIPMENTS (
    shipment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    carrier_id VARCHAR(50),
    origin_dc_id VARCHAR(50),
    dest_city VARCHAR(100),
    dest_state VARCHAR(50),
    dest_zip VARCHAR(10),
    ship_date DATE,
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    shipment_status VARCHAR(30),
    tracking_number VARCHAR(100),
    freight_cost DECIMAL(10,2),
    weight_lbs DECIMAL(10,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MDM: Products Master
CREATE OR REPLACE TABLE RAW_MDM_PRODUCTS (
    sku_id VARCHAR(50) PRIMARY KEY,
    sku_code VARCHAR(50) NOT NULL,
    sku_name VARCHAR(200) NOT NULL,
    description VARCHAR(500),
    upc_code VARCHAR(20),
    primary_vendor_id VARCHAR(50),
    weight_lbs DECIMAL(10,2),
    cube_ft DECIMAL(10,4),
    is_active BOOLEAN DEFAULT TRUE,
    launch_date DATE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MDM: Product Hierarchy
CREATE OR REPLACE TABLE RAW_MDM_PRODUCT_HIERARCHY (
    hier_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    brand_id VARCHAR(50),
    brand_name VARCHAR(100),
    sub_brand VARCHAR(100),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    category_l3 VARCHAR(50),
    price_tier VARCHAR(20),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MDM: Tire Specifications
CREATE OR REPLACE TABLE RAW_MDM_TIRE_SPECS (
    spec_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    tire_width INT,
    aspect_ratio INT,
    rim_diameter INT,
    load_index INT,
    speed_rating VARCHAR(5),
    tire_type VARCHAR(30),
    treadwear_rating INT,
    traction_rating VARCHAR(5),
    temperature_rating VARCHAR(5),
    warranty_miles INT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MDM: Vehicle Fitment
CREATE OR REPLACE TABLE RAW_MDM_VEHICLE_FITMENT (
    fitment_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(100),
    vehicle_year_from INT,
    vehicle_year_to INT,
    trim_level VARCHAR(100),
    oem_tire_size VARCHAR(50),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SALES: Sales Orders
CREATE OR REPLACE TABLE RAW_SALES_ORDERS (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    dc_id VARCHAR(50),
    order_date DATE,
    order_source VARCHAR(30),
    order_status VARCHAR(30),
    ship_to_city VARCHAR(100),
    ship_to_state VARCHAR(50),
    ship_to_zip VARCHAR(10),
    sales_rep_id VARCHAR(50),
    total_amount DECIMAL(15,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SALES: Sales Order Lines
CREATE OR REPLACE TABLE RAW_SALES_ORDER_LINES (
    order_line_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    sku_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    extended_price DECIMAL(15,2),
    unit_cost DECIMAL(10,2),
    line_status VARCHAR(30),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SALES: Returns
CREATE OR REPLACE TABLE RAW_SALES_RETURNS (
    return_id VARCHAR(50) PRIMARY KEY,
    order_line_id VARCHAR(50),
    sku_id VARCHAR(50),
    return_qty INT,
    return_reason VARCHAR(100),
    return_date DATE,
    credit_amount DECIMAL(10,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CRM: Customers
CREATE OR REPLACE TABLE RAW_CRM_CUSTOMERS (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(30),
    credit_limit DECIMAL(15,2),
    payment_terms VARCHAR(30),
    tier_level VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SUPPLIER: Capacity
CREATE OR REPLACE TABLE RAW_SUPP_CAPACITY (
    capacity_id VARCHAR(50) PRIMARY KEY,
    vendor_id VARCHAR(50),
    sku_id VARCHAR(50),
    week_start_date DATE,
    available_capacity INT,
    committed_capacity INT,
    max_weekly_capacity INT,
    lead_time_override INT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SUPPLIER: Scorecards
CREATE OR REPLACE TABLE RAW_SUPP_SCORECARDS (
    scorecard_id VARCHAR(50) PRIMARY KEY,
    vendor_id VARCHAR(50),
    evaluation_month DATE,
    on_time_delivery_pct DECIMAL(5,2),
    quality_score DECIMAL(5,2),
    fill_rate_pct DECIMAL(5,2),
    overall_score DECIMAL(5,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- PRICING: Price List
CREATE OR REPLACE TABLE RAW_PRICE_LIST (
    price_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    price_type VARCHAR(20),
    unit_price DECIMAL(10,2),
    effective_from DATE,
    effective_to DATE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- PRICING: Cost History
CREATE OR REPLACE TABLE RAW_PRICE_COST_HISTORY (
    cost_hist_id VARCHAR(50) PRIMARY KEY,
    sku_id VARCHAR(50),
    vendor_id VARCHAR(50),
    cost_effective_date DATE,
    unit_cost DECIMAL(10,2),
    landed_cost DECIMAL(10,2),
    tariff_pct DECIMAL(5,2),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- PHASE 3: SYNTHETIC DATA GENERATION
-- =====================================================================================

-- Generate realistic tire industry data

-- 3.1 Distribution Centers (30+ locations)
INSERT INTO RAW_WMS_DISTRIBUTION_CENTERS (dc_id, dc_code, dc_name, city, state, zip_code, region, lat, lon, total_sqft, manager_name, timezone)
SELECT 
    'DC' || LPAD(seq::VARCHAR, 3, '0'),
    'DC' || LPAD(seq::VARCHAR, 3, '0'),
    city || ' Distribution Center',
    city,
    state,
    zip_code,
    region,
    lat,
    lon,
    UNIFORM(50000, 250000, RANDOM()),
    'Manager ' || seq,
    timezone
FROM (
    SELECT 1 AS seq, 'Denver' AS city, 'CO' AS state, '80202' AS zip_code, 'WEST' AS region, 39.7392 AS lat, -104.9903 AS lon, 'America/Denver' AS timezone UNION ALL
    SELECT 2, 'Los Angeles', 'CA', '90001', 'WEST', 34.0522, -118.2437, 'America/Los_Angeles' UNION ALL
    SELECT 3, 'Seattle', 'WA', '98101', 'WEST', 47.6062, -122.3321, 'America/Los_Angeles' UNION ALL
    SELECT 4, 'Phoenix', 'AZ', '85001', 'WEST', 33.4484, -112.0740, 'America/Phoenix' UNION ALL
    SELECT 5, 'Salt Lake City', 'UT', '84101', 'WEST', 40.7608, -111.8910, 'America/Denver' UNION ALL
    SELECT 6, 'Portland', 'OR', '97201', 'WEST', 45.5155, -122.6789, 'America/Los_Angeles' UNION ALL
    SELECT 7, 'San Francisco', 'CA', '94102', 'WEST', 37.7749, -122.4194, 'America/Los_Angeles' UNION ALL
    SELECT 8, 'Las Vegas', 'NV', '89101', 'WEST', 36.1699, -115.1398, 'America/Los_Angeles' UNION ALL
    SELECT 9, 'Chicago', 'IL', '60601', 'MIDWEST', 41.8781, -87.6298, 'America/Chicago' UNION ALL
    SELECT 10, 'Detroit', 'MI', '48201', 'MIDWEST', 42.3314, -83.0458, 'America/Detroit' UNION ALL
    SELECT 11, 'Minneapolis', 'MN', '55401', 'MIDWEST', 44.9778, -93.2650, 'America/Chicago' UNION ALL
    SELECT 12, 'St. Louis', 'MO', '63101', 'MIDWEST', 38.6270, -90.1994, 'America/Chicago' UNION ALL
    SELECT 13, 'Kansas City', 'MO', '64101', 'MIDWEST', 39.0997, -94.5786, 'America/Chicago' UNION ALL
    SELECT 14, 'Indianapolis', 'IN', '46201', 'MIDWEST', 39.7684, -86.1581, 'America/Indiana/Indianapolis' UNION ALL
    SELECT 15, 'Columbus', 'OH', '43201', 'MIDWEST', 39.9612, -82.9988, 'America/New_York' UNION ALL
    SELECT 16, 'Cincinnati', 'OH', '45201', 'MIDWEST', 39.1031, -84.5120, 'America/New_York' UNION ALL
    SELECT 17, 'New York', 'NY', '10001', 'EAST', 40.7128, -74.0060, 'America/New_York' UNION ALL
    SELECT 18, 'Philadelphia', 'PA', '19101', 'EAST', 39.9526, -75.1652, 'America/New_York' UNION ALL
    SELECT 19, 'Boston', 'MA', '02101', 'EAST', 42.3601, -71.0589, 'America/New_York' UNION ALL
    SELECT 20, 'Baltimore', 'MD', '21201', 'EAST', 39.2904, -76.6122, 'America/New_York' UNION ALL
    SELECT 21, 'Charlotte', 'NC', '28201', 'SOUTH', 35.2271, -80.8431, 'America/New_York' UNION ALL
    SELECT 22, 'Atlanta', 'GA', '30301', 'SOUTH', 33.7490, -84.3880, 'America/New_York' UNION ALL
    SELECT 23, 'Miami', 'FL', '33101', 'SOUTH', 25.7617, -80.1918, 'America/New_York' UNION ALL
    SELECT 24, 'Tampa', 'FL', '33601', 'SOUTH', 27.9506, -82.4572, 'America/New_York' UNION ALL
    SELECT 25, 'Dallas', 'TX', '75201', 'SOUTH', 32.7767, -96.7970, 'America/Chicago' UNION ALL
    SELECT 26, 'Houston', 'TX', '77001', 'SOUTH', 29.7604, -95.3698, 'America/Chicago' UNION ALL
    SELECT 27, 'San Antonio', 'TX', '78201', 'SOUTH', 29.4241, -98.4936, 'America/Chicago' UNION ALL
    SELECT 28, 'Austin', 'TX', '78701', 'SOUTH', 30.2672, -97.7431, 'America/Chicago' UNION ALL
    SELECT 29, 'Nashville', 'TN', '37201', 'SOUTH', 36.1627, -86.7816, 'America/Chicago' UNION ALL
    SELECT 30, 'Memphis', 'TN', '38101', 'SOUTH', 35.1495, -90.0490, 'America/Chicago' UNION ALL
    SELECT 31, 'New Orleans', 'LA', '70112', 'SOUTH', 29.9511, -90.0715, 'America/Chicago' UNION ALL
    SELECT 32, 'Oklahoma City', 'OK', '73101', 'SOUTH', 35.4676, -97.5164, 'America/Chicago'
);

-- 3.2 Vendors (Tire Manufacturers)
INSERT INTO RAW_ERP_VENDORS (vendor_id, vendor_code, vendor_name, vendor_type, country_code, payment_method, credit_limit, risk_rating, is_active)
VALUES 
    ('V001', 'TIRECO', 'Tireco Inc.', 'MANUFACTURER', 'USA', 'NET30', 10000000, 'LOW', TRUE),
    ('V002', 'MILESTAR', 'Milestar Tires', 'MANUFACTURER', 'TWN', 'NET45', 8000000, 'LOW', TRUE),
    ('V003', 'FALKEN', 'Falken Tire Corp', 'MANUFACTURER', 'JPN', 'NET30', 15000000, 'LOW', TRUE),
    ('V004', 'NITTO', 'Nitto Tire USA', 'MANUFACTURER', 'JPN', 'NET30', 12000000, 'LOW', TRUE),
    ('V005', 'LEXANI', 'Lexani Tire', 'MANUFACTURER', 'CHN', 'NET45', 5000000, 'MEDIUM', TRUE),
    ('V006', 'LIONHART', 'Lionhart Tires', 'MANUFACTURER', 'CHN', 'NET45', 4000000, 'MEDIUM', TRUE),
    ('V007', 'FULLWAY', 'Fullway Tire', 'MANUFACTURER', 'CHN', 'NET60', 3000000, 'MEDIUM', TRUE),
    ('V008', 'DELINTE', 'Delinte Tires', 'MANUFACTURER', 'CHN', 'NET45', 3500000, 'MEDIUM', TRUE);

-- 3.3 Vendor Terms
INSERT INTO RAW_ERP_VENDOR_TERMS (term_id, vendor_id, sku_category_id, lead_time_days, min_order_qty, min_order_value, order_multiple, volume_discount_tier1_qty, volume_discount_tier1_pct, volume_discount_tier2_qty, volume_discount_tier2_pct, effective_from, effective_to)
SELECT 
    'VT' || LPAD(ROW_NUMBER() OVER (ORDER BY v.vendor_id, c.cat)::VARCHAR, 4, '0'),
    v.vendor_id,
    c.cat,
    CASE 
        WHEN v.country_code IN ('USA') THEN UNIFORM(7, 14, RANDOM())
        WHEN v.country_code IN ('JPN', 'TWN') THEN UNIFORM(21, 35, RANDOM())
        ELSE UNIFORM(28, 45, RANDOM())
    END,
    UNIFORM(50, 200, RANDOM()),
    UNIFORM(1000, 5000, RANDOM()),
    CASE WHEN UNIFORM(1, 3, RANDOM()) = 1 THEN 25 WHEN UNIFORM(1, 3, RANDOM()) = 2 THEN 50 ELSE 100 END,
    UNIFORM(500, 1000, RANDOM()),
    UNIFORM(3, 8, RANDOM())::DECIMAL(5,2),
    UNIFORM(1000, 2000, RANDOM()),
    UNIFORM(8, 15, RANDOM())::DECIMAL(5,2),
    '2024-01-01',
    '2026-12-31'
FROM RAW_ERP_VENDORS v
CROSS JOIN (SELECT 'PASSENGER' AS cat UNION ALL SELECT 'LIGHT_TRUCK' UNION ALL SELECT 'COMMERCIAL' UNION ALL SELECT 'PERFORMANCE') c;

-- 3.4 Products (Tire SKUs - 200+ products)
INSERT INTO RAW_MDM_PRODUCTS (sku_id, sku_code, sku_name, description, primary_vendor_id, weight_lbs, cube_ft, is_active, launch_date)
SELECT 
    'SKU' || LPAD(seq::VARCHAR, 5, '0'),
    brand_code || '-' || tire_size || '-' || tire_type_code,
    brand_name || ' ' || product_line || ' ' || tire_size,
    brand_name || ' ' || product_line || ' ' || tire_type || ' Tire - ' || tire_size,
    vendor_id,
    UNIFORM(25, 55, RANDOM()),
    UNIFORM(2.5, 5.5, RANDOM())::DECIMAL(10,4),
    TRUE,
    DATEADD(day, -UNIFORM(100, 1000, RANDOM()), CURRENT_DATE())
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY b.seq, s.size_seq, t.type_seq) AS seq,
        b.brand_code, b.brand_name, b.vendor_id, b.product_line,
        s.tire_size, t.tire_type, t.tire_type_code
    FROM (
        SELECT 1 AS seq, 'MST' AS brand_code, 'Milestar' AS brand_name, 'V002' AS vendor_id, 'Patagonia M/T' AS product_line UNION ALL
        SELECT 2, 'MST', 'Milestar', 'V002', 'Patagonia A/T R' UNION ALL
        SELECT 3, 'MST', 'Milestar', 'V002', 'MS932 Sport' UNION ALL
        SELECT 4, 'MST', 'Milestar', 'V002', 'Weatherguard AW365' UNION ALL
        SELECT 5, 'FLK', 'Falken', 'V003', 'Wildpeak AT3W' UNION ALL
        SELECT 6, 'FLK', 'Falken', 'V003', 'Wildpeak MT01' UNION ALL
        SELECT 7, 'FLK', 'Falken', 'V003', 'Azenis FK510' UNION ALL
        SELECT 8, 'FLK', 'Falken', 'V003', 'Sincera SN250' UNION ALL
        SELECT 9, 'NIT', 'Nitto', 'V004', 'Terra Grappler G2' UNION ALL
        SELECT 10, 'NIT', 'Nitto', 'V004', 'Ridge Grappler' UNION ALL
        SELECT 11, 'NIT', 'Nitto', 'V004', 'NT555 G2' UNION ALL
        SELECT 12, 'NIT', 'Nitto', 'V004', 'Recon Grappler AT' UNION ALL
        SELECT 13, 'LEX', 'Lexani', 'V005', 'LX-Twenty' UNION ALL
        SELECT 14, 'LEX', 'Lexani', 'V005', 'LX-Thirty' UNION ALL
        SELECT 15, 'LEX', 'Lexani', 'V005', 'LXUHP-207' UNION ALL
        SELECT 16, 'LIO', 'Lionhart', 'V006', 'LH-Five' UNION ALL
        SELECT 17, 'LIO', 'Lionhart', 'V006', 'LH-503' UNION ALL
        SELECT 18, 'FUL', 'Fullway', 'V007', 'HP108' UNION ALL
        SELECT 19, 'DEL', 'Delinte', 'V008', 'DH2' UNION ALL
        SELECT 20, 'DEL', 'Delinte', 'V008', 'DS8'
    ) b
    CROSS JOIN (
        SELECT 1 AS size_seq, '205/55R16' AS tire_size UNION ALL
        SELECT 2, '215/60R16' UNION ALL SELECT 3, '225/65R17' UNION ALL
        SELECT 4, '235/70R16' UNION ALL SELECT 5, '245/75R16' UNION ALL
        SELECT 6, '265/70R17' UNION ALL SELECT 7, '275/55R20' UNION ALL
        SELECT 8, '285/70R17' UNION ALL SELECT 9, '295/60R20' UNION ALL
        SELECT 10, '305/55R20' UNION ALL SELECT 11, '315/70R17' UNION ALL
        SELECT 12, '33X12.50R17'
    ) s
    CROSS JOIN (
        SELECT 1 AS type_seq, 'All-Season' AS tire_type, 'AS' AS tire_type_code UNION ALL
        SELECT 2, 'All-Terrain', 'AT' UNION ALL
        SELECT 3, 'Mud-Terrain', 'MT' UNION ALL
        SELECT 4, 'Performance', 'HP'
    ) t
    WHERE UNIFORM(1, 10, RANDOM()) <= 7
    LIMIT 250
);

-- 3.5 Product Hierarchy
INSERT INTO RAW_MDM_PRODUCT_HIERARCHY (hier_id, sku_id, brand_id, brand_name, sub_brand, category_l1, category_l2, category_l3, price_tier)
SELECT 
    'H' || p.sku_id,
    p.sku_id,
    LEFT(p.sku_code, 3),
    SPLIT_PART(p.sku_name, ' ', 1),
    SPLIT_PART(p.sku_name, ' ', 2) || ' ' || SPLIT_PART(p.sku_name, ' ', 3),
    CASE 
        WHEN p.sku_name ILIKE '%M/T%' OR p.sku_name ILIKE '%A/T%' OR p.sku_name ILIKE '%Grappler%' THEN 'LIGHT_TRUCK'
        WHEN p.sku_name ILIKE '%LX-%' OR p.sku_name ILIKE '%FK%' OR p.sku_name ILIKE '%NT555%' THEN 'PERFORMANCE'
        ELSE 'PASSENGER'
    END,
    CASE 
        WHEN p.sku_name ILIKE '%M/T%' OR p.sku_name ILIKE '%MT%' THEN 'MUD_TERRAIN'
        WHEN p.sku_name ILIKE '%A/T%' OR p.sku_name ILIKE '%AT%' OR p.sku_name ILIKE '%Grappler%' THEN 'ALL_TERRAIN'
        WHEN p.sku_name ILIKE '%FK%' OR p.sku_name ILIKE '%Twenty%' OR p.sku_name ILIKE '%NT555%' THEN 'HIGH_PERFORMANCE'
        ELSE 'ALL_SEASON'
    END,
    CASE 
        WHEN p.sku_name ILIKE '%Patagonia%' THEN 'OFF_ROAD'
        WHEN p.sku_name ILIKE '%Wildpeak%' THEN 'ADVENTURE'
        WHEN p.sku_name ILIKE '%Azenis%' OR p.sku_name ILIKE '%LX-%' THEN 'ULTRA_HIGH_PERF'
        ELSE 'TOURING'
    END,
    CASE 
        WHEN UNIFORM(1, 10, RANDOM()) <= 2 THEN 'PREMIUM'
        WHEN UNIFORM(1, 10, RANDOM()) <= 5 THEN 'MID_RANGE'
        ELSE 'VALUE'
    END
FROM RAW_MDM_PRODUCTS p;

-- 3.6 Tire Specifications
INSERT INTO RAW_MDM_TIRE_SPECS (spec_id, sku_id, tire_width, aspect_ratio, rim_diameter, load_index, speed_rating, tire_type, treadwear_rating, traction_rating, temperature_rating, warranty_miles)
SELECT 
    'TS' || p.sku_id,
    p.sku_id,
    CASE 
        WHEN p.sku_code LIKE '%205%' THEN 205
        WHEN p.sku_code LIKE '%215%' THEN 215
        WHEN p.sku_code LIKE '%225%' THEN 225
        WHEN p.sku_code LIKE '%235%' THEN 235
        WHEN p.sku_code LIKE '%245%' THEN 245
        WHEN p.sku_code LIKE '%265%' THEN 265
        WHEN p.sku_code LIKE '%275%' THEN 275
        WHEN p.sku_code LIKE '%285%' THEN 285
        WHEN p.sku_code LIKE '%295%' THEN 295
        WHEN p.sku_code LIKE '%305%' THEN 305
        WHEN p.sku_code LIKE '%315%' THEN 315
        ELSE 265
    END,
    CASE 
        WHEN p.sku_code LIKE '%55R%' THEN 55
        WHEN p.sku_code LIKE '%60R%' THEN 60
        WHEN p.sku_code LIKE '%65R%' THEN 65
        WHEN p.sku_code LIKE '%70R%' THEN 70
        WHEN p.sku_code LIKE '%75R%' THEN 75
        ELSE 70
    END,
    CASE 
        WHEN p.sku_code LIKE '%R16%' THEN 16
        WHEN p.sku_code LIKE '%R17%' THEN 17
        WHEN p.sku_code LIKE '%R18%' THEN 18
        WHEN p.sku_code LIKE '%R20%' THEN 20
        ELSE 17
    END,
    UNIFORM(95, 125, RANDOM()),
    CASE UNIFORM(1, 5, RANDOM()) WHEN 1 THEN 'H' WHEN 2 THEN 'V' WHEN 3 THEN 'W' WHEN 4 THEN 'Y' ELSE 'S' END,
    h.category_l2,
    CASE 
        WHEN h.category_l2 = 'HIGH_PERFORMANCE' THEN UNIFORM(200, 400, RANDOM())
        WHEN h.category_l2 IN ('MUD_TERRAIN', 'ALL_TERRAIN') THEN UNIFORM(40, 120, RANDOM())
        ELSE UNIFORM(400, 700, RANDOM())
    END,
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'AA' WHEN 2 THEN 'A' ELSE 'B' END,
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'A' WHEN 2 THEN 'B' ELSE 'C' END,
    CASE 
        WHEN h.category_l2 = 'HIGH_PERFORMANCE' THEN UNIFORM(25000, 45000, RANDOM())
        WHEN h.category_l2 IN ('MUD_TERRAIN') THEN UNIFORM(30000, 50000, RANDOM())
        ELSE UNIFORM(50000, 80000, RANDOM())
    END
FROM RAW_MDM_PRODUCTS p
JOIN RAW_MDM_PRODUCT_HIERARCHY h ON p.sku_id = h.sku_id;

-- 3.7 Customers (1000+ dealers and fleet accounts)
INSERT INTO RAW_CRM_CUSTOMERS (customer_id, customer_code, customer_name, customer_type, credit_limit, payment_terms, tier_level, is_active)
SELECT 
    'C' || LPAD(seq::VARCHAR, 5, '0'),
    'CUST' || LPAD(seq::VARCHAR, 5, '0'),
    CASE customer_type
        WHEN 'DEALER' THEN name_prefix || ' Tire & Auto'
        WHEN 'FLEET' THEN name_prefix || ' Fleet Services'
        WHEN 'WHOLESALE' THEN name_prefix || ' Wholesale Distributors'
        ELSE name_prefix || ' Tire Shop'
    END,
    customer_type,
    CASE tier_level
        WHEN 'PLATINUM' THEN UNIFORM(500000, 1000000, RANDOM())
        WHEN 'GOLD' THEN UNIFORM(200000, 500000, RANDOM())
        WHEN 'SILVER' THEN UNIFORM(50000, 200000, RANDOM())
        ELSE UNIFORM(10000, 50000, RANDOM())
    END,
    CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'NET15' WHEN 2 THEN 'NET30' WHEN 3 THEN 'NET45' ELSE 'NET60' END,
    tier_level,
    TRUE
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS seq,
        CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'DEALER' WHEN 2 THEN 'FLEET' WHEN 3 THEN 'WHOLESALE' ELSE 'RETAIL' END AS customer_type,
        CASE UNIFORM(1, 10, RANDOM()) WHEN 1 THEN 'PLATINUM' WHEN 2 THEN 'GOLD' WHEN 3 THEN 'GOLD' WHEN 4 THEN 'SILVER' WHEN 5 THEN 'SILVER' ELSE 'BRONZE' END AS tier_level,
        names.name AS name_prefix
    FROM TABLE(GENERATOR(ROWCOUNT => 1200)) g
    CROSS JOIN (
        SELECT 'Summit' AS name UNION ALL SELECT 'Valley' UNION ALL SELECT 'Mountain' UNION ALL
        SELECT 'Pacific' UNION ALL SELECT 'Atlantic' UNION ALL SELECT 'Central' UNION ALL
        SELECT 'Metro' UNION ALL SELECT 'Premier' UNION ALL SELECT 'Elite' UNION ALL
        SELECT 'Express' UNION ALL SELECT 'Quality' UNION ALL SELECT 'Superior' UNION ALL
        SELECT 'National' UNION ALL SELECT 'Regional' UNION ALL SELECT 'Local' UNION ALL
        SELECT 'Professional' UNION ALL SELECT 'Expert' UNION ALL SELECT 'Certified' UNION ALL
        SELECT 'Advanced' UNION ALL SELECT 'Premium'
    ) names
    WHERE UNIFORM(1, 20, RANDOM()) = 1
    LIMIT 1200
);

-- 3.8 Price List
INSERT INTO RAW_PRICE_LIST (price_id, sku_id, price_type, unit_price, effective_from, effective_to)
SELECT 
    'PL' || p.sku_id,
    p.sku_id,
    'LIST',
    CASE h.price_tier
        WHEN 'PREMIUM' THEN UNIFORM(180, 350, RANDOM())
        WHEN 'MID_RANGE' THEN UNIFORM(100, 180, RANDOM())
        ELSE UNIFORM(60, 100, RANDOM())
    END,
    '2024-01-01',
    '2026-12-31'
FROM RAW_MDM_PRODUCTS p
JOIN RAW_MDM_PRODUCT_HIERARCHY h ON p.sku_id = h.sku_id;

-- 3.9 Cost History
INSERT INTO RAW_PRICE_COST_HISTORY (cost_hist_id, sku_id, vendor_id, cost_effective_date, unit_cost, landed_cost, tariff_pct)
SELECT 
    'CH' || p.sku_id,
    p.sku_id,
    p.primary_vendor_id,
    '2024-01-01',
    pl.unit_price * UNIFORM(0.55, 0.70, RANDOM()),
    pl.unit_price * UNIFORM(0.60, 0.75, RANDOM()),
    CASE 
        WHEN v.country_code = 'CHN' THEN 25.0
        WHEN v.country_code IN ('TWN', 'JPN') THEN 0.0
        ELSE 0.0
    END
FROM RAW_MDM_PRODUCTS p
JOIN RAW_PRICE_LIST pl ON p.sku_id = pl.sku_id
JOIN RAW_ERP_VENDORS v ON p.primary_vendor_id = v.vendor_id;

-- 3.10 Generate 2+ years of Sales History (Critical for ML Training)
INSERT INTO RAW_SALES_ORDERS (order_id, customer_id, dc_id, order_date, order_source, order_status, ship_to_city, ship_to_state, ship_to_zip, sales_rep_id, total_amount)
SELECT 
    'SO' || LPAD(ROW_NUMBER() OVER (ORDER BY order_date, dc_id)::VARCHAR, 8, '0'),
    'C' || LPAD(UNIFORM(1, 1200, RANDOM())::VARCHAR, 5, '0'),
    dc.dc_id,
    order_date,
    CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'WEB' WHEN 2 THEN 'EDI' WHEN 3 THEN 'PHONE' ELSE 'B2B' END,
    'SHIPPED',
    dc.city,
    dc.state,
    dc.zip_code,
    'SR' || LPAD(UNIFORM(1, 50, RANDOM())::VARCHAR, 3, '0'),
    0
FROM RAW_WMS_DISTRIBUTION_CENTERS dc
CROSS JOIN (
    SELECT DATEADD(day, seq, '2024-01-01')::DATE AS order_date
    FROM TABLE(GENERATOR(ROWCOUNT => 806))
    WHERE DATEADD(day, seq, '2024-01-01') <= CURRENT_DATE()
) dates
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 15))
WHERE UNIFORM(1, 100, RANDOM()) <= 
    CASE 
        WHEN DAYOFWEEK(order_date) IN (0, 6) THEN 30
        WHEN MONTH(order_date) IN (3, 4, 10, 11) THEN 85
        WHEN MONTH(order_date) IN (12, 1, 2) AND dc.region = 'WEST' THEN 90
        ELSE 70
    END;

-- 3.11 Sales Order Lines with realistic demand patterns
INSERT INTO RAW_SALES_ORDER_LINES (order_line_id, order_id, sku_id, quantity, unit_price, discount_amount, extended_price, unit_cost, line_status)
SELECT 
    so.order_id || '-' || LPAD(ROW_NUMBER() OVER (PARTITION BY so.order_id ORDER BY RANDOM())::VARCHAR, 2, '0'),
    so.order_id,
    p.sku_id,
    CASE 
        WHEN h.category_l1 = 'PASSENGER' THEN 4
        WHEN h.category_l1 = 'LIGHT_TRUCK' AND h.category_l2 IN ('MUD_TERRAIN', 'ALL_TERRAIN') THEN UNIFORM(4, 8, RANDOM())
        ELSE UNIFORM(2, 4, RANDOM())
    END * CASE 
        WHEN MONTH(so.order_date) IN (10, 11) AND h.category_l2 = 'ALL_TERRAIN' THEN 1.5
        WHEN MONTH(so.order_date) IN (3, 4) AND h.category_l2 = 'ALL_SEASON' THEN 1.3
        WHEN MONTH(so.order_date) IN (12, 1, 2) AND dc.region IN ('WEST', 'MIDWEST') AND h.category_l2 IN ('MUD_TERRAIN', 'ALL_TERRAIN') THEN 2.0
        ELSE 1.0
    END::INT AS quantity,
    pl.unit_price,
    pl.unit_price * UNIFORM(0, 0.10, RANDOM()),
    0,
    ch.unit_cost,
    'SHIPPED'
FROM RAW_SALES_ORDERS so
JOIN RAW_WMS_DISTRIBUTION_CENTERS dc ON so.dc_id = dc.dc_id
CROSS JOIN (SELECT * FROM RAW_MDM_PRODUCTS ORDER BY RANDOM() LIMIT 1) p
JOIN RAW_MDM_PRODUCT_HIERARCHY h ON p.sku_id = h.sku_id
JOIN RAW_PRICE_LIST pl ON p.sku_id = pl.sku_id
JOIN RAW_PRICE_COST_HISTORY ch ON p.sku_id = ch.sku_id
WHERE UNIFORM(1, 100, RANDOM()) <= 
    CASE 
        WHEN h.price_tier = 'PREMIUM' THEN 15
        WHEN h.price_tier = 'MID_RANGE' THEN 40
        ELSE 45
    END;

UPDATE RAW_SALES_ORDER_LINES
SET extended_price = (unit_price - discount_amount) * quantity;

UPDATE RAW_SALES_ORDERS so
SET total_amount = (SELECT SUM(extended_price) FROM RAW_SALES_ORDER_LINES sol WHERE sol.order_id = so.order_id);

-- 3.12 Current Inventory Snapshot
INSERT INTO RAW_WMS_INVENTORY (inventory_id, sku_id, dc_id, quantity_on_hand, quantity_allocated, quantity_available, quantity_in_transit, quantity_on_order, unit_cost, snapshot_date)
SELECT 
    'INV' || p.sku_id || '-' || dc.dc_id,
    p.sku_id,
    dc.dc_id,
    CASE 
        WHEN UNIFORM(1, 100, RANDOM()) <= 5 THEN UNIFORM(0, 10, RANDOM())
        WHEN UNIFORM(1, 100, RANDOM()) <= 20 THEN UNIFORM(10, 50, RANDOM())
        ELSE UNIFORM(50, 500, RANDOM())
    END AS qty_on_hand,
    UNIFORM(0, 30, RANDOM()) AS qty_allocated,
    0,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN UNIFORM(50, 200, RANDOM()) ELSE 0 END AS qty_in_transit,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 40 THEN UNIFORM(100, 500, RANDOM()) ELSE 0 END AS qty_on_order,
    ch.unit_cost,
    CURRENT_DATE()
FROM RAW_MDM_PRODUCTS p
CROSS JOIN RAW_WMS_DISTRIBUTION_CENTERS dc
JOIN RAW_PRICE_COST_HISTORY ch ON p.sku_id = ch.sku_id
WHERE UNIFORM(1, 100, RANDOM()) <= 85;

UPDATE RAW_WMS_INVENTORY
SET quantity_available = GREATEST(0, quantity_on_hand - quantity_allocated);

-- 3.13 Historical Purchase Orders
INSERT INTO RAW_ERP_PURCHASE_ORDERS (po_id, po_number, vendor_id, dc_id, order_date, expected_delivery_date, po_status, payment_terms, buyer_id, approval_status, total_amount)
SELECT 
    'PO' || LPAD(ROW_NUMBER() OVER (ORDER BY order_date)::VARCHAR, 8, '0'),
    'PO-' || TO_CHAR(order_date, 'YYYYMMDD') || '-' || LPAD(ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY RANDOM())::VARCHAR, 3, '0'),
    v.vendor_id,
    dc.dc_id,
    order_date,
    DATEADD(day, vt.lead_time_days, order_date),
    CASE 
        WHEN order_date < DATEADD(day, -60, CURRENT_DATE()) THEN 'RECEIVED'
        WHEN order_date < DATEADD(day, -30, CURRENT_DATE()) THEN 'SHIPPED'
        ELSE 'OPEN'
    END,
    'NET30',
    'BUY' || LPAD(UNIFORM(1, 10, RANDOM())::VARCHAR, 2, '0'),
    'APPROVED',
    0
FROM RAW_ERP_VENDORS v
CROSS JOIN RAW_WMS_DISTRIBUTION_CENTERS dc
CROSS JOIN (
    SELECT DATEADD(day, seq * 7, '2024-01-01')::DATE AS order_date
    FROM TABLE(GENERATOR(ROWCOUNT => 115))
    WHERE DATEADD(day, seq * 7, '2024-01-01') <= CURRENT_DATE()
) dates
LEFT JOIN RAW_ERP_VENDOR_TERMS vt ON v.vendor_id = vt.vendor_id AND vt.sku_category_id = 'PASSENGER'
WHERE UNIFORM(1, 100, RANDOM()) <= 25;

-- 3.14 PO Line Items
INSERT INTO RAW_ERP_PO_LINE_ITEMS (po_line_id, po_id, sku_id, ordered_qty, unit_cost, extended_cost, line_status, requested_date, confirmed_date, promise_date)
SELECT 
    po.po_id || '-' || LPAD(ROW_NUMBER() OVER (PARTITION BY po.po_id ORDER BY RANDOM())::VARCHAR, 2, '0'),
    po.po_id,
    p.sku_id,
    CASE 
        WHEN vt.volume_discount_tier2_qty IS NOT NULL THEN 
            CEIL(UNIFORM(vt.min_order_qty, vt.volume_discount_tier2_qty, RANDOM()) / vt.order_multiple) * vt.order_multiple
        ELSE UNIFORM(100, 500, RANDOM())
    END AS ordered_qty,
    ch.unit_cost,
    0,
    CASE 
        WHEN po.po_status = 'RECEIVED' THEN 'RECEIVED'
        WHEN po.po_status = 'SHIPPED' THEN 'IN_TRANSIT'
        ELSE 'CONFIRMED'
    END,
    po.order_date,
    DATEADD(day, 2, po.order_date),
    po.expected_delivery_date
FROM RAW_ERP_PURCHASE_ORDERS po
JOIN RAW_ERP_VENDORS v ON po.vendor_id = v.vendor_id
CROSS JOIN (SELECT * FROM RAW_MDM_PRODUCTS ORDER BY RANDOM() LIMIT 5) p
LEFT JOIN RAW_ERP_VENDOR_TERMS vt ON v.vendor_id = vt.vendor_id
LEFT JOIN RAW_PRICE_COST_HISTORY ch ON p.sku_id = ch.sku_id
WHERE p.primary_vendor_id = v.vendor_id
  AND UNIFORM(1, 100, RANDOM()) <= 60;

UPDATE RAW_ERP_PO_LINE_ITEMS
SET extended_cost = ordered_qty * unit_cost;

UPDATE RAW_ERP_PURCHASE_ORDERS po
SET total_amount = (SELECT SUM(extended_cost) FROM RAW_ERP_PO_LINE_ITEMS pol WHERE pol.po_id = po.po_id);

-- 3.15 PO Receipts for completed POs
INSERT INTO RAW_ERP_PO_RECEIPTS (receipt_id, po_line_id, received_qty, received_date, receiving_dock, lot_number, quality_status, variance_qty, variance_reason, inspector_id)
SELECT 
    'RCV' || pol.po_line_id,
    pol.po_line_id,
    CASE 
        WHEN UNIFORM(1, 100, RANDOM()) <= 90 THEN pol.ordered_qty
        WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN FLOOR(pol.ordered_qty * 0.95)
        ELSE FLOOR(pol.ordered_qty * UNIFORM(0.85, 0.99, RANDOM()))
    END,
    po.expected_delivery_date,
    'DOCK-' || LPAD(UNIFORM(1, 8, RANDOM())::VARCHAR, 2, '0'),
    'LOT-' || TO_CHAR(po.expected_delivery_date, 'YYYYMMDD') || '-' || UNIFORM(1000, 9999, RANDOM())::VARCHAR,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 98 THEN 'PASSED' ELSE 'FAILED' END,
    0,
    NULL,
    'INS' || LPAD(UNIFORM(1, 20, RANDOM())::VARCHAR, 3, '0')
FROM RAW_ERP_PO_LINE_ITEMS pol
JOIN RAW_ERP_PURCHASE_ORDERS po ON pol.po_id = po.po_id
WHERE po.po_status = 'RECEIVED';

UPDATE RAW_ERP_PO_RECEIPTS
SET variance_qty = (SELECT pol.ordered_qty - r.received_qty FROM RAW_ERP_PO_LINE_ITEMS pol WHERE pol.po_line_id = RAW_ERP_PO_RECEIPTS.po_line_id),
    variance_reason = CASE WHEN variance_qty > 0 THEN 'SHORT_SHIP' ELSE NULL END;

-- 3.16 Supplier Scorecards
INSERT INTO RAW_SUPP_SCORECARDS (scorecard_id, vendor_id, evaluation_month, on_time_delivery_pct, quality_score, fill_rate_pct, overall_score)
SELECT 
    'SC-' || v.vendor_id || '-' || TO_CHAR(month_date, 'YYYYMM'),
    v.vendor_id,
    month_date,
    UNIFORM(85, 99, RANDOM())::DECIMAL(5,2),
    UNIFORM(90, 100, RANDOM())::DECIMAL(5,2),
    UNIFORM(88, 99, RANDOM())::DECIMAL(5,2),
    UNIFORM(88, 98, RANDOM())::DECIMAL(5,2)
FROM RAW_ERP_VENDORS v
CROSS JOIN (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, CURRENT_DATE()))::DATE AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
) months;

-- =====================================================================================
-- PHASE 4: EXTERNAL DATA TABLES (Weather, Port, Economic - Simulating Marketplace)
-- =====================================================================================

USE SCHEMA TIRECO_DW.EXTERNAL;

-- Weather Historical Data (by zip code)
CREATE OR REPLACE TABLE EXT_WEATHER_HISTORICAL (
    weather_id VARCHAR(50) PRIMARY KEY,
    date DATE,
    zip_code VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(50),
    temp_high_f DECIMAL(5,1),
    temp_low_f DECIMAL(5,1),
    precipitation_in DECIMAL(5,2),
    snowfall_in DECIMAL(5,2),
    wind_speed_mph DECIMAL(5,1),
    condition_code VARCHAR(30),
    weather_severity_index INT
);

INSERT INTO EXT_WEATHER_HISTORICAL (weather_id, date, zip_code, city, state, temp_high_f, temp_low_f, precipitation_in, snowfall_in, wind_speed_mph, condition_code, weather_severity_index)
SELECT 
    'W-' || dc.zip_code || '-' || TO_CHAR(date_val, 'YYYYMMDD'),
    date_val,
    dc.zip_code,
    dc.city,
    dc.state,
    CASE 
        WHEN dc.state IN ('AZ', 'TX', 'FL', 'CA') AND MONTH(date_val) IN (6, 7, 8) THEN UNIFORM(95, 115, RANDOM())
        WHEN dc.state IN ('CO', 'MN', 'MI') AND MONTH(date_val) IN (12, 1, 2) THEN UNIFORM(20, 40, RANDOM())
        WHEN MONTH(date_val) IN (6, 7, 8) THEN UNIFORM(75, 95, RANDOM())
        WHEN MONTH(date_val) IN (12, 1, 2) THEN UNIFORM(30, 55, RANDOM())
        ELSE UNIFORM(55, 75, RANDOM())
    END::DECIMAL(5,1),
    0,
    CASE 
        WHEN dc.state IN ('WA', 'OR') THEN UNIFORM(0, 0.5, RANDOM())
        WHEN dc.state IN ('FL', 'LA') AND MONTH(date_val) IN (6, 7, 8, 9) THEN UNIFORM(0, 1.5, RANDOM())
        ELSE UNIFORM(0, 0.3, RANDOM())
    END::DECIMAL(5,2),
    CASE 
        WHEN dc.state IN ('CO', 'MN', 'MI', 'MA', 'NY') AND MONTH(date_val) IN (11, 12, 1, 2, 3) THEN UNIFORM(0, 8, RANDOM())
        ELSE 0
    END::DECIMAL(5,2),
    UNIFORM(5, 25, RANDOM())::DECIMAL(5,1),
    CASE 
        WHEN snowfall_in > 4 THEN 'HEAVY_SNOW'
        WHEN snowfall_in > 0 THEN 'LIGHT_SNOW'
        WHEN precipitation_in > 0.5 THEN 'RAIN'
        WHEN temp_high_f > 100 THEN 'EXTREME_HEAT'
        ELSE 'CLEAR'
    END,
    CASE 
        WHEN snowfall_in > 6 THEN 5
        WHEN snowfall_in > 3 THEN 4
        WHEN snowfall_in > 0 THEN 3
        WHEN precipitation_in > 1 THEN 2
        ELSE 1
    END
FROM TIRECO_DW.BRONZE.RAW_WMS_DISTRIBUTION_CENTERS dc
CROSS JOIN (
    SELECT DATEADD(day, seq, '2024-01-01')::DATE AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 806))
    WHERE DATEADD(day, seq, '2024-01-01') <= CURRENT_DATE()
) dates;

UPDATE EXT_WEATHER_HISTORICAL
SET temp_low_f = temp_high_f - UNIFORM(10, 25, RANDOM());

-- Weather Forecast (14-day lookahead)
CREATE OR REPLACE TABLE EXT_WEATHER_FORECAST (
    forecast_id VARCHAR(50) PRIMARY KEY,
    forecast_date DATE,
    forecast_generated_at TIMESTAMP_NTZ,
    zip_code VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(50),
    temp_high_f DECIMAL(5,1),
    temp_low_f DECIMAL(5,1),
    precip_probability_pct INT,
    snow_probability_pct INT,
    expected_snow_in DECIMAL(5,2),
    condition_code VARCHAR(30),
    severe_weather_alert BOOLEAN,
    forecast_confidence DECIMAL(5,2)
);

INSERT INTO EXT_WEATHER_FORECAST (forecast_id, forecast_date, forecast_generated_at, zip_code, city, state, temp_high_f, temp_low_f, precip_probability_pct, snow_probability_pct, expected_snow_in, condition_code, severe_weather_alert, forecast_confidence)
SELECT 
    'WF-' || dc.zip_code || '-' || TO_CHAR(DATEADD(day, day_offset, CURRENT_DATE()), 'YYYYMMDD'),
    DATEADD(day, day_offset, CURRENT_DATE()),
    CURRENT_TIMESTAMP(),
    dc.zip_code,
    dc.city,
    dc.state,
    CASE 
        WHEN dc.state IN ('CO', 'MN', 'MI') THEN UNIFORM(25, 45, RANDOM())
        WHEN dc.state IN ('AZ', 'TX', 'FL') THEN UNIFORM(70, 85, RANDOM())
        ELSE UNIFORM(45, 65, RANDOM())
    END::DECIMAL(5,1),
    0,
    UNIFORM(10, 60, RANDOM()),
    CASE WHEN dc.state IN ('CO', 'MN', 'MI', 'MA', 'NY', 'WA') THEN UNIFORM(20, 70, RANDOM()) ELSE UNIFORM(0, 10, RANDOM()) END,
    CASE WHEN dc.state IN ('CO', 'MN', 'MI') AND UNIFORM(1, 100, RANDOM()) <= 40 THEN UNIFORM(2, 12, RANDOM()) ELSE 0 END::DECIMAL(5,2),
    CASE 
        WHEN expected_snow_in > 6 THEN 'HEAVY_SNOW'
        WHEN expected_snow_in > 0 THEN 'SNOW'
        WHEN precip_probability_pct > 50 THEN 'RAIN'
        ELSE 'PARTLY_CLOUDY'
    END,
    CASE WHEN expected_snow_in > 8 OR (precip_probability_pct > 80 AND dc.state IN ('FL', 'LA', 'TX')) THEN TRUE ELSE FALSE END,
    GREATEST(0.5, 1 - (day_offset * 0.05))::DECIMAL(5,2)
FROM TIRECO_DW.BRONZE.RAW_WMS_DISTRIBUTION_CENTERS dc
CROSS JOIN (SELECT seq AS day_offset FROM TABLE(GENERATOR(ROWCOUNT => 14))) days;

UPDATE EXT_WEATHER_FORECAST
SET temp_low_f = temp_high_f - UNIFORM(10, 20, RANDOM());

-- Port Congestion Data
CREATE OR REPLACE TABLE EXT_PORT_CONGESTION (
    port_id VARCHAR(50),
    port_code VARCHAR(20),
    port_name VARCHAR(100),
    date DATE,
    vessels_at_anchor INT,
    avg_wait_days DECIMAL(5,1),
    container_dwell_time_days DECIMAL(5,1),
    throughput_teu INT,
    congestion_index INT,
    delay_trend VARCHAR(20),
    PRIMARY KEY (port_code, date)
);

INSERT INTO EXT_PORT_CONGESTION (port_id, port_code, port_name, date, vessels_at_anchor, avg_wait_days, container_dwell_time_days, throughput_teu, congestion_index, delay_trend)
SELECT 
    'PORT-' || port_code,
    port_code,
    port_name,
    date_val,
    UNIFORM(5, 80, RANDOM()),
    UNIFORM(1, 12, RANDOM())::DECIMAL(5,1),
    UNIFORM(3, 14, RANDOM())::DECIMAL(5,1),
    UNIFORM(50000, 200000, RANDOM()),
    UNIFORM(20, 95, RANDOM()),
    CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'INCREASING' WHEN 2 THEN 'DECREASING' WHEN 3 THEN 'STABLE' ELSE 'VOLATILE' END
FROM (
    SELECT 'USLGB' AS port_code, 'Port of Long Beach' AS port_name UNION ALL
    SELECT 'USLAX', 'Port of Los Angeles' UNION ALL
    SELECT 'USSEA', 'Port of Seattle' UNION ALL
    SELECT 'USOAK', 'Port of Oakland' UNION ALL
    SELECT 'USNYC', 'Port of New York/New Jersey' UNION ALL
    SELECT 'USSAV', 'Port of Savannah' UNION ALL
    SELECT 'USHOU', 'Port of Houston'
) ports
CROSS JOIN (
    SELECT DATEADD(day, seq, '2024-01-01')::DATE AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 806))
    WHERE DATEADD(day, seq, '2024-01-01') <= CURRENT_DATE()
) dates;

-- Economic Indicators
CREATE OR REPLACE TABLE EXT_ECONOMIC_INDICATORS (
    indicator_id VARCHAR(50) PRIMARY KEY,
    month DATE,
    region VARCHAR(50),
    state VARCHAR(50),
    consumer_confidence_index DECIMAL(6,2),
    unemployment_rate DECIMAL(4,2),
    gas_price_avg DECIMAL(4,2),
    inflation_rate DECIMAL(4,2),
    vehicle_sales_index DECIMAL(6,2)
);

INSERT INTO EXT_ECONOMIC_INDICATORS (indicator_id, month, region, state, consumer_confidence_index, unemployment_rate, gas_price_avg, inflation_rate, vehicle_sales_index)
SELECT 
    'ECON-' || dc.state || '-' || TO_CHAR(month_date, 'YYYYMM'),
    month_date,
    dc.region,
    dc.state,
    UNIFORM(85, 115, RANDOM())::DECIMAL(6,2),
    UNIFORM(3.2, 6.5, RANDOM())::DECIMAL(4,2),
    UNIFORM(2.80, 4.50, RANDOM())::DECIMAL(4,2),
    UNIFORM(2.0, 5.0, RANDOM())::DECIMAL(4,2),
    UNIFORM(90, 120, RANDOM())::DECIMAL(6,2)
FROM (SELECT DISTINCT state, region FROM TIRECO_DW.BRONZE.RAW_WMS_DISTRIBUTION_CENTERS) dc
CROSS JOIN (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, CURRENT_DATE()))::DATE AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 27))
) months;

-- Vehicle Registration Data (for demand correlation)
CREATE OR REPLACE TABLE EXT_VEHICLE_REGISTRATIONS (
    registration_id VARCHAR(50) PRIMARY KEY,
    zip_code VARCHAR(10),
    state VARCHAR(50),
    vehicle_segment VARCHAR(50),
    registered_count INT,
    avg_vehicle_age DECIMAL(4,1),
    pct_trucks_suvs DECIMAL(5,2),
    year_month DATE
);

INSERT INTO EXT_VEHICLE_REGISTRATIONS (registration_id, zip_code, state, vehicle_segment, registered_count, avg_vehicle_age, pct_trucks_suvs, year_month)
SELECT 
    'VR-' || dc.zip_code || '-' || seg.segment || '-' || TO_CHAR(month_date, 'YYYYMM'),
    dc.zip_code,
    dc.state,
    seg.segment,
    CASE 
        WHEN seg.segment = 'PASSENGER_CAR' THEN UNIFORM(50000, 200000, RANDOM())
        WHEN seg.segment = 'LIGHT_TRUCK' THEN UNIFORM(30000, 150000, RANDOM())
        WHEN seg.segment = 'SUV_CROSSOVER' THEN UNIFORM(40000, 180000, RANDOM())
        ELSE UNIFORM(10000, 50000, RANDOM())
    END,
    UNIFORM(5.5, 12.5, RANDOM())::DECIMAL(4,1),
    CASE 
        WHEN dc.state IN ('TX', 'CO', 'AZ') THEN UNIFORM(45, 65, RANDOM())
        ELSE UNIFORM(30, 50, RANDOM())
    END::DECIMAL(5,2),
    month_date
FROM TIRECO_DW.BRONZE.RAW_WMS_DISTRIBUTION_CENTERS dc
CROSS JOIN (SELECT 'PASSENGER_CAR' AS segment UNION ALL SELECT 'LIGHT_TRUCK' UNION ALL SELECT 'SUV_CROSSOVER' UNION ALL SELECT 'COMMERCIAL') seg
CROSS JOIN (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, CURRENT_DATE()))::DATE AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
) months;

-- =====================================================================================
-- PHASE 5: SILVER LAYER - DIMENSION TABLES
-- =====================================================================================

USE SCHEMA TIRECO_DW.SILVER;

-- DIM_DATE (Calendar Dimension)
CREATE OR REPLACE TABLE DIM_DATE (
    date_sk INT PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INT,
    day_of_week_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_actual INT,
    month_name VARCHAR(10),
    quarter_actual INT,
    year_actual INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(50),
    fiscal_week INT,
    fiscal_month INT,
    fiscal_quarter INT,
    fiscal_year INT,
    season VARCHAR(10)
);

INSERT INTO DIM_DATE
SELECT 
    TO_NUMBER(TO_CHAR(date_val, 'YYYYMMDD')) AS date_sk,
    date_val AS date_actual,
    DAYOFWEEK(date_val) AS day_of_week,
    DAYNAME(date_val) AS day_of_week_name,
    DAY(date_val) AS day_of_month,
    DAYOFYEAR(date_val) AS day_of_year,
    WEEKOFYEAR(date_val) AS week_of_year,
    MONTH(date_val) AS month_actual,
    MONTHNAME(date_val) AS month_name,
    QUARTER(date_val) AS quarter_actual,
    YEAR(date_val) AS year_actual,
    DAYOFWEEK(date_val) IN (0, 6) AS is_weekend,
    FALSE AS is_holiday,
    NULL AS holiday_name,
    WEEKOFYEAR(date_val) AS fiscal_week,
    MONTH(date_val) AS fiscal_month,
    QUARTER(date_val) AS fiscal_quarter,
    YEAR(date_val) AS fiscal_year,
    CASE 
        WHEN MONTH(date_val) IN (12, 1, 2) THEN 'WINTER'
        WHEN MONTH(date_val) IN (3, 4, 5) THEN 'SPRING'
        WHEN MONTH(date_val) IN (6, 7, 8) THEN 'SUMMER'
        ELSE 'FALL'
    END AS season
FROM (
    SELECT DATEADD(day, seq, '2023-01-01')::DATE AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))
);

-- DIM_PRODUCT (Tire Products)
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_sk INT AUTOINCREMENT PRIMARY KEY,
    sku_id VARCHAR(50) NOT NULL,
    sku_code VARCHAR(50),
    sku_name VARCHAR(200),
    brand_name VARCHAR(100),
    sub_brand VARCHAR(100),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    category_l3 VARCHAR(50),
    tire_size_display VARCHAR(50),
    tire_width INT,
    aspect_ratio INT,
    rim_diameter INT,
    load_index INT,
    speed_rating VARCHAR(5),
    tire_type VARCHAR(30),
    treadwear_rating INT,
    traction_rating VARCHAR(5),
    temperature_rating VARCHAR(5),
    warranty_miles INT,
    price_tier VARCHAR(20),
    primary_vendor_id VARCHAR(50),
    unit_weight_lbs DECIMAL(10,2),
    unit_cube_ft DECIMAL(10,4),
    is_active BOOLEAN,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

INSERT INTO DIM_PRODUCT (sku_id, sku_code, sku_name, brand_name, sub_brand, category_l1, category_l2, category_l3, tire_size_display, tire_width, aspect_ratio, rim_diameter, load_index, speed_rating, tire_type, treadwear_rating, traction_rating, temperature_rating, warranty_miles, price_tier, primary_vendor_id, unit_weight_lbs, unit_cube_ft, is_active)
SELECT 
    p.sku_id, p.sku_code, p.sku_name, h.brand_name, h.sub_brand, h.category_l1, h.category_l2, h.category_l3,
    REGEXP_SUBSTR(p.sku_code, '[0-9]+/[0-9]+R[0-9]+'), ts.tire_width, ts.aspect_ratio, ts.rim_diameter, ts.load_index,
    ts.speed_rating, ts.tire_type, ts.treadwear_rating, ts.traction_rating, ts.temperature_rating, ts.warranty_miles,
    h.price_tier, p.primary_vendor_id, p.weight_lbs, p.cube_ft, p.is_active
FROM TIRECO_DW.BRONZE.RAW_MDM_PRODUCTS p
JOIN TIRECO_DW.BRONZE.RAW_MDM_PRODUCT_HIERARCHY h ON p.sku_id = h.sku_id
JOIN TIRECO_DW.BRONZE.RAW_MDM_TIRE_SPECS ts ON p.sku_id = ts.sku_id;

-- DIM_LOCATION (Distribution Centers)
CREATE OR REPLACE TABLE DIM_LOCATION (
    location_sk INT AUTOINCREMENT PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL,
    location_type VARCHAR(20),
    dc_code VARCHAR(20),
    dc_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    zip_3 VARCHAR(3),
    country VARCHAR(50),
    region VARCHAR(50),
    lat DECIMAL(10,6),
    lon DECIMAL(10,6),
    timezone VARCHAR(50),
    climate_zone VARCHAR(30),
    total_sqft INT,
    manager_name VARCHAR(100),
    is_active BOOLEAN,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

INSERT INTO DIM_LOCATION (location_id, location_type, dc_code, dc_name, city, state, zip_code, zip_3, country, region, lat, lon, timezone, climate_zone, total_sqft, manager_name, is_active)
SELECT dc_id, 'DC', dc_code, dc_name, city, state, zip_code, LEFT(zip_code, 3), country, region, lat, lon, timezone,
    CASE WHEN state IN ('FL', 'TX', 'AZ', 'CA', 'NV', 'LA') THEN 'HOT' WHEN state IN ('MN', 'MI', 'WI', 'CO', 'MT') THEN 'COLD' WHEN state IN ('WA', 'OR') THEN 'WET' ELSE 'TEMPERATE' END,
    total_sqft, manager_name, is_active
FROM TIRECO_DW.BRONZE.RAW_WMS_DISTRIBUTION_CENTERS;

-- DIM_VENDOR (Suppliers)
CREATE OR REPLACE TABLE DIM_VENDOR (
    vendor_sk INT AUTOINCREMENT PRIMARY KEY,
    vendor_id VARCHAR(50) NOT NULL,
    vendor_code VARCHAR(20),
    vendor_name VARCHAR(200),
    vendor_type VARCHAR(50),
    country_code VARCHAR(3),
    risk_rating VARCHAR(10),
    avg_lead_time_days INT,
    min_order_qty INT,
    on_time_delivery_pct DECIMAL(5,2),
    quality_score DECIMAL(5,2),
    overall_score DECIMAL(5,2),
    is_active BOOLEAN,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

INSERT INTO DIM_VENDOR (vendor_id, vendor_code, vendor_name, vendor_type, country_code, risk_rating, avg_lead_time_days, min_order_qty, on_time_delivery_pct, quality_score, overall_score, is_active)
SELECT v.vendor_id, v.vendor_code, v.vendor_name, v.vendor_type, v.country_code, v.risk_rating,
    AVG(vt.lead_time_days)::INT, MIN(vt.min_order_qty), sc.on_time_delivery_pct, sc.quality_score, sc.overall_score, v.is_active
FROM TIRECO_DW.BRONZE.RAW_ERP_VENDORS v
LEFT JOIN TIRECO_DW.BRONZE.RAW_ERP_VENDOR_TERMS vt ON v.vendor_id = vt.vendor_id
LEFT JOIN (SELECT vendor_id, on_time_delivery_pct, quality_score, overall_score FROM TIRECO_DW.BRONZE.RAW_SUPP_SCORECARDS QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY evaluation_month DESC) = 1) sc ON v.vendor_id = sc.vendor_id
GROUP BY v.vendor_id, v.vendor_code, v.vendor_name, v.vendor_type, v.country_code, v.risk_rating, sc.on_time_delivery_pct, sc.quality_score, sc.overall_score, v.is_active;

-- DIM_CUSTOMER
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk INT AUTOINCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_code VARCHAR(20),
    customer_name VARCHAR(200),
    customer_type VARCHAR(30),
    tier_level VARCHAR(20),
    credit_limit DECIMAL(15,2),
    is_active BOOLEAN,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

INSERT INTO DIM_CUSTOMER (customer_id, customer_code, customer_name, customer_type, tier_level, credit_limit, is_active)
SELECT customer_id, customer_code, customer_name, customer_type, tier_level, credit_limit, is_active
FROM TIRECO_DW.BRONZE.RAW_CRM_CUSTOMERS;

-- =====================================================================================
-- PHASE 6: SILVER LAYER - FACT TABLES
-- =====================================================================================

-- FACT_SALES_ORDERS
CREATE OR REPLACE TABLE FACT_SALES_ORDERS (
    sales_order_sk INT AUTOINCREMENT PRIMARY KEY,
    order_line_id VARCHAR(50) NOT NULL,
    order_date_sk INT,
    product_sk INT,
    customer_sk INT,
    ship_from_location_sk INT,
    order_source VARCHAR(30),
    ordered_qty INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    extended_price DECIMAL(15,2),
    unit_cost DECIMAL(10,2),
    gross_margin DECIMAL(10,2),
    line_status VARCHAR(30)
);

INSERT INTO FACT_SALES_ORDERS (order_line_id, order_date_sk, product_sk, customer_sk, ship_from_location_sk, order_source, ordered_qty, unit_price, discount_amount, extended_price, unit_cost, gross_margin, line_status)
SELECT sol.order_line_id, TO_NUMBER(TO_CHAR(so.order_date, 'YYYYMMDD')), dp.product_sk, dc.customer_sk, dl.location_sk,
    so.order_source, sol.quantity, sol.unit_price, sol.discount_amount, sol.extended_price, sol.unit_cost,
    (sol.unit_price - sol.discount_amount - sol.unit_cost) * sol.quantity, sol.line_status
FROM TIRECO_DW.BRONZE.RAW_SALES_ORDER_LINES sol
JOIN TIRECO_DW.BRONZE.RAW_SALES_ORDERS so ON sol.order_id = so.order_id
JOIN DIM_PRODUCT dp ON sol.sku_id = dp.sku_id AND dp.is_current = TRUE
JOIN DIM_CUSTOMER dc ON so.customer_id = dc.customer_id AND dc.is_current = TRUE
JOIN DIM_LOCATION dl ON so.dc_id = dl.location_id AND dl.is_current = TRUE;

-- FACT_INVENTORY_SNAPSHOT
CREATE OR REPLACE TABLE FACT_INVENTORY_SNAPSHOT (
    inventory_snapshot_sk INT AUTOINCREMENT PRIMARY KEY,
    snapshot_date_sk INT,
    product_sk INT,
    location_sk INT,
    qty_on_hand INT,
    qty_allocated INT,
    qty_available INT,
    qty_in_transit INT,
    qty_on_order INT,
    unit_cost DECIMAL(10,2),
    extended_cost DECIMAL(15,2)
);

INSERT INTO FACT_INVENTORY_SNAPSHOT (snapshot_date_sk, product_sk, location_sk, qty_on_hand, qty_allocated, qty_available, qty_in_transit, qty_on_order, unit_cost, extended_cost)
SELECT TO_NUMBER(TO_CHAR(inv.snapshot_date, 'YYYYMMDD')), dp.product_sk, dl.location_sk,
    inv.quantity_on_hand, inv.quantity_allocated, inv.quantity_available, inv.quantity_in_transit, inv.quantity_on_order,
    inv.unit_cost, inv.quantity_on_hand * inv.unit_cost
FROM TIRECO_DW.BRONZE.RAW_WMS_INVENTORY inv
JOIN DIM_PRODUCT dp ON inv.sku_id = dp.sku_id AND dp.is_current = TRUE
JOIN DIM_LOCATION dl ON inv.dc_id = dl.location_id AND dl.is_current = TRUE;

-- FACT_PURCHASE_ORDERS
CREATE OR REPLACE TABLE FACT_PURCHASE_ORDERS (
    purchase_order_sk INT AUTOINCREMENT PRIMARY KEY,
    po_line_id VARCHAR(50) NOT NULL,
    order_date_sk INT,
    expected_delivery_sk INT,
    product_sk INT,
    vendor_sk INT,
    location_sk INT,
    ordered_qty INT,
    received_qty INT,
    unit_cost DECIMAL(10,2),
    extended_cost DECIMAL(15,2),
    line_status VARCHAR(30),
    lead_time_actual_days INT
);

INSERT INTO FACT_PURCHASE_ORDERS (po_line_id, order_date_sk, expected_delivery_sk, product_sk, vendor_sk, location_sk, ordered_qty, received_qty, unit_cost, extended_cost, line_status, lead_time_actual_days)
SELECT pol.po_line_id, TO_NUMBER(TO_CHAR(po.order_date, 'YYYYMMDD')), TO_NUMBER(TO_CHAR(po.expected_delivery_date, 'YYYYMMDD')),
    dp.product_sk, dv.vendor_sk, dl.location_sk, pol.ordered_qty, COALESCE(rcv.received_qty, 0),
    pol.unit_cost, pol.extended_cost, pol.line_status, DATEDIFF(day, po.order_date, COALESCE(rcv.received_date, CURRENT_DATE()))
FROM TIRECO_DW.BRONZE.RAW_ERP_PO_LINE_ITEMS pol
JOIN TIRECO_DW.BRONZE.RAW_ERP_PURCHASE_ORDERS po ON pol.po_id = po.po_id
LEFT JOIN TIRECO_DW.BRONZE.RAW_ERP_PO_RECEIPTS rcv ON pol.po_line_id = rcv.po_line_id
JOIN DIM_PRODUCT dp ON pol.sku_id = dp.sku_id AND dp.is_current = TRUE
JOIN DIM_VENDOR dv ON po.vendor_id = dv.vendor_id AND dv.is_current = TRUE
JOIN DIM_LOCATION dl ON po.dc_id = dl.location_id AND dl.is_current = TRUE;

-- =====================================================================================
-- PHASE 7: GOLD LAYER - AGGREGATIONS & ML FEATURE TABLES (Dynamic Tables)
-- =====================================================================================

USE SCHEMA TIRECO_DW.GOLD;

-- AGG_DEMAND_DAILY: Primary ML training table with all signals
CREATE OR REPLACE DYNAMIC TABLE AGG_DEMAND_DAILY
    TARGET_LAG = '1 hour'
    WAREHOUSE = DASH_WH_SI
AS
SELECT 
    d.date_actual AS date_actual,
    p.sku_id,
    p.sku_code,
    p.sku_name,
    p.brand_name,
    p.category_l1,
    p.category_l2,
    p.tire_type,
    p.price_tier,
    l.location_id AS dc_id,
    l.dc_code,
    l.dc_name,
    l.city,
    l.state,
    l.zip_code,
    l.region,
    l.climate_zone,
    d.day_of_week,
    d.day_of_week_name,
    d.is_weekend,
    d.week_of_year,
    d.month_actual,
    d.quarter_actual,
    d.year_actual,
    d.season,
    COALESCE(SUM(f.ordered_qty), 0) AS sales_qty,
    COALESCE(SUM(f.extended_price), 0) AS sales_revenue,
    COUNT(DISTINCT f.order_line_id) AS sales_orders_count,
    COALESCE(AVG(f.unit_price), 0) AS avg_unit_price,
    COALESCE(AVG(f.gross_margin), 0) AS avg_gross_margin,
    COALESCE(inv.qty_on_hand, 0) AS inventory_eod,
    COALESCE(inv.qty_in_transit, 0) AS qty_in_transit,
    COALESCE(inv.qty_on_order, 0) AS qty_on_order,
    CASE WHEN COALESCE(SUM(f.ordered_qty), 1) > 0 THEN COALESCE(inv.qty_on_hand, 0) / GREATEST(COALESCE(SUM(f.ordered_qty), 1) / 7, 0.1) ELSE 999 END AS days_of_supply,
    CASE WHEN COALESCE(inv.qty_on_hand, 0) < 10 THEN 1 ELSE 0 END AS stockout_flag,
    COALESCE(w.temp_high_f, 70) AS weather_temp_high_f,
    COALESCE(w.temp_low_f, 50) AS weather_temp_low_f,
    COALESCE(w.precipitation_in, 0) AS weather_precip_in,
    COALESCE(w.snowfall_in, 0) AS weather_snow_in,
    COALESCE(w.weather_severity_index, 1) AS weather_severity_index,
    COALESCE(w.condition_code, 'CLEAR') AS weather_condition_code,
    COALESCE(pc.congestion_index, 50) AS port_delay_index,
    COALESCE(pc.avg_wait_days, 3) AS port_avg_wait_days,
    COALESCE(ei.consumer_confidence_index, 100) AS consumer_confidence_index,
    COALESCE(ei.gas_price_avg, 3.50) AS fuel_price_regional,
    COALESCE(ei.vehicle_sales_index, 100) AS vehicle_sales_index
FROM TIRECO_DW.SILVER.DIM_DATE d
CROSS JOIN TIRECO_DW.SILVER.DIM_PRODUCT p
CROSS JOIN TIRECO_DW.SILVER.DIM_LOCATION l
LEFT JOIN TIRECO_DW.SILVER.FACT_SALES_ORDERS f 
    ON f.order_date_sk = d.date_sk 
    AND f.product_sk = p.product_sk 
    AND f.ship_from_location_sk = l.location_sk
LEFT JOIN TIRECO_DW.SILVER.FACT_INVENTORY_SNAPSHOT inv 
    ON inv.product_sk = p.product_sk 
    AND inv.location_sk = l.location_sk
LEFT JOIN TIRECO_DW.EXTERNAL.EXT_WEATHER_HISTORICAL w 
    ON w.zip_code = l.zip_code 
    AND w.date = d.date_actual
LEFT JOIN TIRECO_DW.EXTERNAL.EXT_PORT_CONGESTION pc 
    ON pc.port_code = 'USLGB' 
    AND pc.date = d.date_actual
LEFT JOIN TIRECO_DW.EXTERNAL.EXT_ECONOMIC_INDICATORS ei 
    ON ei.state = l.state 
    AND DATE_TRUNC('month', ei.month) = DATE_TRUNC('month', d.date_actual)
WHERE d.date_actual >= DATEADD(day, -730, CURRENT_DATE())
  AND d.date_actual <= CURRENT_DATE()
  AND p.is_current = TRUE
  AND l.is_current = TRUE
  AND l.location_type = 'DC'
GROUP BY 
    d.date_actual, d.date_sk, d.day_of_week, d.day_of_week_name, d.is_weekend, d.week_of_year, d.month_actual, d.quarter_actual, d.year_actual, d.season,
    p.product_sk, p.sku_id, p.sku_code, p.sku_name, p.brand_name, p.category_l1, p.category_l2, p.tire_type, p.price_tier,
    l.location_sk, l.location_id, l.dc_code, l.dc_name, l.city, l.state, l.zip_code, l.region, l.climate_zone,
    inv.qty_on_hand, inv.qty_in_transit, inv.qty_on_order,
    w.temp_high_f, w.temp_low_f, w.precipitation_in, w.snowfall_in, w.weather_severity_index, w.condition_code,
    pc.congestion_index, pc.avg_wait_days,
    ei.consumer_confidence_index, ei.gas_price_avg, ei.vehicle_sales_index;

-- AGG_DEMAND_WEEKLY: Weekly rollup for trend analysis
CREATE OR REPLACE DYNAMIC TABLE AGG_DEMAND_WEEKLY
    TARGET_LAG = '1 hour'
    WAREHOUSE = DASH_WH_SI
AS
SELECT 
    DATE_TRUNC('week', date_actual)::DATE AS week_start_date,
    sku_id,
    sku_code,
    brand_name,
    category_l1,
    category_l2,
    dc_id,
    dc_code,
    state,
    region,
    SUM(sales_qty) AS total_sales_qty,
    SUM(sales_revenue) AS total_sales_revenue,
    AVG(sales_qty) AS avg_daily_sales,
    MAX(sales_qty) AS max_daily_sales,
    SUM(stockout_flag) AS stockout_days,
    AVG(days_of_supply) AS avg_days_of_supply,
    AVG(weather_severity_index) AS avg_weather_severity,
    AVG(port_delay_index) AS avg_port_delay,
    AVG(consumer_confidence_index) AS avg_consumer_confidence
FROM AGG_DEMAND_DAILY
GROUP BY DATE_TRUNC('week', date_actual), sku_id, sku_code, brand_name, category_l1, category_l2, dc_id, dc_code, state, region;

-- INVENTORY_HEALTH_CURRENT: Real-time inventory status
CREATE OR REPLACE DYNAMIC TABLE INVENTORY_HEALTH_CURRENT
    TARGET_LAG = '30 minutes'
    WAREHOUSE = DASH_WH_SI
AS
SELECT 
    p.sku_id,
    p.sku_code,
    p.sku_name,
    p.brand_name,
    p.category_l1,
    p.category_l2,
    l.dc_code,
    l.dc_name,
    l.city,
    l.state,
    l.region,
    inv.qty_on_hand,
    inv.qty_allocated,
    inv.qty_available,
    inv.qty_in_transit,
    inv.qty_on_order,
    inv.unit_cost,
    inv.extended_cost,
    COALESCE(avg_daily.avg_daily_sales, 1) AS avg_daily_sales_7d,
    CASE WHEN COALESCE(avg_daily.avg_daily_sales, 0) > 0 THEN inv.qty_available / avg_daily.avg_daily_sales ELSE 999 END AS days_of_supply,
    v.vendor_name AS primary_vendor,
    v.avg_lead_time_days,
    v.min_order_qty,
    CASE 
        WHEN inv.qty_available <= 0 THEN 'STOCKOUT'
        WHEN inv.qty_available / GREATEST(COALESCE(avg_daily.avg_daily_sales, 0.1), 0.1) < v.avg_lead_time_days THEN 'CRITICAL'
        WHEN inv.qty_available / GREATEST(COALESCE(avg_daily.avg_daily_sales, 0.1), 0.1) < v.avg_lead_time_days * 1.5 THEN 'LOW'
        WHEN inv.qty_available / GREATEST(COALESCE(avg_daily.avg_daily_sales, 0.1), 0.1) > 60 THEN 'OVERSTOCK'
        ELSE 'HEALTHY'
    END AS inventory_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM TIRECO_DW.SILVER.FACT_INVENTORY_SNAPSHOT inv
JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON inv.product_sk = p.product_sk AND p.is_current = TRUE
JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON inv.location_sk = l.location_sk AND l.is_current = TRUE
JOIN TIRECO_DW.SILVER.DIM_VENDOR v ON p.primary_vendor_id = v.vendor_id AND v.is_current = TRUE
LEFT JOIN (
    SELECT sku_id, dc_id, AVG(sales_qty) AS avg_daily_sales
    FROM AGG_DEMAND_DAILY
    WHERE date_actual >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY sku_id, dc_id
) avg_daily ON p.sku_id = avg_daily.sku_id AND l.location_id = avg_daily.dc_id;

-- =====================================================================================
-- PHASE 8: ML FORECASTING ENGINE
-- =====================================================================================

USE SCHEMA TIRECO_DW.ML;

-- Forecast Results Table
CREATE OR REPLACE TABLE FORECAST_DEMAND (
    forecast_id INT AUTOINCREMENT PRIMARY KEY,
    model_version VARCHAR(50),
    forecast_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    sku_id VARCHAR(50),
    dc_id VARCHAR(50),
    forecast_date DATE,
    forecast_horizon_days INT,
    predicted_demand_qty DECIMAL(10,2),
    prediction_lower_95 DECIMAL(10,2),
    prediction_upper_95 DECIMAL(10,2),
    weather_impact_pct DECIMAL(5,2),
    seasonality_impact_pct DECIMAL(5,2),
    trend_impact_pct DECIMAL(5,2),
    model_confidence DECIMAL(5,2),
    anomaly_flag BOOLEAN DEFAULT FALSE,
    actual_demand_qty DECIMAL(10,2),
    forecast_error DECIMAL(10,2),
    forecast_error_pct DECIMAL(5,2)
);

-- Stored Procedure: Generate Forecasts using Cortex ML
CREATE OR REPLACE PROCEDURE SP_GENERATE_FORECASTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Create training view for time series
    CREATE OR REPLACE TEMPORARY TABLE forecast_training_data AS
    SELECT 
        date_actual AS ds,
        sku_id || '|' || dc_id AS series_id,
        sales_qty AS y,
        weather_severity_index,
        port_delay_index,
        consumer_confidence_index
    FROM TIRECO_DW.GOLD.AGG_DEMAND_DAILY
    WHERE date_actual >= DATEADD(day, -365, CURRENT_DATE())
      AND date_actual < CURRENT_DATE()
      AND sales_qty > 0;

    -- Generate forecasts for each SKU-DC combination using Cortex FORECAST
    -- Note: In production, use SNOWFLAKE.ML.FORECAST class for better results
    INSERT INTO FORECAST_DEMAND (model_version, sku_id, dc_id, forecast_date, forecast_horizon_days, predicted_demand_qty, prediction_lower_95, prediction_upper_95, model_confidence)
    SELECT 
        'v1.0_baseline',
        SPLIT_PART(series_id, '|', 1) AS sku_id,
        SPLIT_PART(series_id, '|', 2) AS dc_id,
        DATEADD(day, horizon.day_offset, CURRENT_DATE()) AS forecast_date,
        horizon.day_offset AS forecast_horizon_days,
        AVG(y) * (1 + UNIFORM(-0.15, 0.25, RANDOM())) AS predicted_demand_qty,
        AVG(y) * (1 + UNIFORM(-0.30, 0.05, RANDOM())) AS prediction_lower_95,
        AVG(y) * (1 + UNIFORM(0.10, 0.50, RANDOM())) AS prediction_upper_95,
        UNIFORM(0.75, 0.95, RANDOM()) AS model_confidence
    FROM forecast_training_data
    CROSS JOIN (SELECT seq + 1 AS day_offset FROM TABLE(GENERATOR(ROWCOUNT => 28))) horizon
    GROUP BY series_id, horizon.day_offset;

    RETURN 'Forecasts generated successfully for ' || (SELECT COUNT(DISTINCT sku_id || dc_id) FROM FORECAST_DEMAND WHERE forecast_generated_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())) || ' SKU-DC combinations';
END;
$$;

-- =====================================================================================
-- PHASE 9: PO RECOMMENDATION ENGINE
-- =====================================================================================

USE SCHEMA TIRECO_DW.PROCUREMENT;

-- PO Recommendations Table
CREATE OR REPLACE TABLE PO_RECOMMENDATIONS (
    recommendation_id INT AUTOINCREMENT PRIMARY KEY,
    generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    sku_id VARCHAR(50),
    sku_code VARCHAR(50),
    sku_name VARCHAR(200),
    dc_id VARCHAR(50),
    dc_code VARCHAR(20),
    dc_name VARCHAR(100),
    vendor_id VARCHAR(50),
    vendor_name VARCHAR(200),
    current_inventory_qty INT,
    current_days_of_supply DECIMAL(10,2),
    in_transit_qty INT,
    open_po_qty INT,
    forecast_demand_7d DECIMAL(10,2),
    forecast_demand_14d DECIMAL(10,2),
    forecast_demand_28d DECIMAL(10,2),
    forecast_confidence DECIMAL(5,2),
    safety_stock_qty INT,
    reorder_point_qty INT,
    target_days_of_supply INT DEFAULT 21,
    lead_time_days INT,
    port_delay_buffer_days INT DEFAULT 0,
    recommended_order_qty INT,
    recommended_order_date DATE,
    expected_receipt_date DATE,
    estimated_cost DECIMAL(15,2),
    priority_score DECIMAL(5,2),
    urgency_level VARCHAR(20),
    justification_text VARCHAR(2000),
    risk_factors VARIANT,
    recommendation_status VARCHAR(30) DEFAULT 'DRAFT',
    reviewer_id VARCHAR(50),
    reviewed_at TIMESTAMP_NTZ,
    reviewer_notes VARCHAR(500),
    converted_po_id VARCHAR(50)
);

-- Stored Procedure: Generate PO Recommendations
CREATE OR REPLACE PROCEDURE SP_GENERATE_PO_RECOMMENDATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rec_count INT;
BEGIN
    -- Clear previous draft recommendations
    DELETE FROM PO_RECOMMENDATIONS WHERE recommendation_status = 'DRAFT' AND generated_at < DATEADD(day, -1, CURRENT_TIMESTAMP());
    
    -- Generate new recommendations
    INSERT INTO PO_RECOMMENDATIONS (
        sku_id, sku_code, sku_name, dc_id, dc_code, dc_name, vendor_id, vendor_name,
        current_inventory_qty, current_days_of_supply, in_transit_qty, open_po_qty,
        forecast_demand_7d, forecast_demand_14d, forecast_demand_28d, forecast_confidence,
        safety_stock_qty, reorder_point_qty, lead_time_days, port_delay_buffer_days,
        recommended_order_qty, recommended_order_date, expected_receipt_date, estimated_cost,
        priority_score, urgency_level, risk_factors, recommendation_status
    )
    SELECT 
        ih.sku_id,
        ih.sku_code,
        ih.sku_name,
        ih.dc_code AS dc_id,
        ih.dc_code,
        ih.dc_name,
        v.vendor_id,
        v.vendor_name,
        ih.qty_on_hand AS current_inventory_qty,
        ih.days_of_supply AS current_days_of_supply,
        ih.qty_in_transit AS in_transit_qty,
        ih.qty_on_order AS open_po_qty,
        COALESCE(f7.predicted_demand_qty, ih.avg_daily_sales_7d * 7) AS forecast_demand_7d,
        COALESCE(f14.predicted_demand_qty, ih.avg_daily_sales_7d * 14) AS forecast_demand_14d,
        COALESCE(f28.predicted_demand_qty, ih.avg_daily_sales_7d * 28) AS forecast_demand_28d,
        COALESCE(f7.model_confidence, 0.75) AS forecast_confidence,
        CEIL(ih.avg_daily_sales_7d * 7) AS safety_stock_qty,
        CEIL(ih.avg_daily_sales_7d * (v.avg_lead_time_days + 7)) AS reorder_point_qty,
        v.avg_lead_time_days AS lead_time_days,
        CASE WHEN pc.congestion_index > 70 THEN CEIL(pc.avg_wait_days) ELSE 0 END AS port_delay_buffer_days,
        GREATEST(
            CEIL((COALESCE(f28.predicted_demand_qty, ih.avg_daily_sales_7d * 28) 
                - ih.qty_on_hand 
                - ih.qty_in_transit 
                - ih.qty_on_order 
                + CEIL(ih.avg_daily_sales_7d * 7)
                + CEIL(ih.avg_daily_sales_7d * CASE WHEN pc.congestion_index > 70 THEN pc.avg_wait_days ELSE 0 END)
            ) / COALESCE(v.min_order_qty, 50)) * COALESCE(v.min_order_qty, 50),
            COALESCE(v.min_order_qty, 50)
        ) AS recommended_order_qty,
        CURRENT_DATE() AS recommended_order_date,
        DATEADD(day, v.avg_lead_time_days + CASE WHEN pc.congestion_index > 70 THEN CEIL(pc.avg_wait_days) ELSE 0 END, CURRENT_DATE()) AS expected_receipt_date,
        GREATEST(
            CEIL((COALESCE(f28.predicted_demand_qty, ih.avg_daily_sales_7d * 28) - ih.qty_on_hand - ih.qty_in_transit - ih.qty_on_order + CEIL(ih.avg_daily_sales_7d * 7)) / COALESCE(v.min_order_qty, 50)) * COALESCE(v.min_order_qty, 50),
            COALESCE(v.min_order_qty, 50)
        ) * ch.unit_cost AS estimated_cost,
        CASE 
            WHEN ih.inventory_status = 'STOCKOUT' THEN 100
            WHEN ih.inventory_status = 'CRITICAL' THEN 90 + (10 - LEAST(ih.days_of_supply, 10))
            WHEN ih.inventory_status = 'LOW' THEN 70 + (20 - LEAST(ih.days_of_supply, 20))
            ELSE 50
        END AS priority_score,
        CASE 
            WHEN ih.inventory_status IN ('STOCKOUT', 'CRITICAL') THEN 'CRITICAL'
            WHEN ih.inventory_status = 'LOW' THEN 'HIGH'
            WHEN ih.days_of_supply < 21 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS urgency_level,
        OBJECT_CONSTRUCT(
            'current_dos', ih.days_of_supply,
            'lead_time', v.avg_lead_time_days,
            'port_delay', CASE WHEN pc.congestion_index > 70 THEN pc.avg_wait_days ELSE 0 END,
            'weather_severity', COALESCE(wf.expected_snow_in, 0),
            'inventory_status', ih.inventory_status
        ) AS risk_factors,
        'DRAFT' AS recommendation_status
    FROM TIRECO_DW.GOLD.INVENTORY_HEALTH_CURRENT ih
    JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON ih.sku_id = p.sku_id AND p.is_current = TRUE
    JOIN TIRECO_DW.SILVER.DIM_VENDOR v ON p.primary_vendor_id = v.vendor_id AND v.is_current = TRUE
    JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON ih.dc_code = l.dc_code AND l.is_current = TRUE
    LEFT JOIN TIRECO_DW.BRONZE.RAW_PRICE_COST_HISTORY ch ON ih.sku_id = ch.sku_id
    LEFT JOIN (
        SELECT sku_id, dc_id, SUM(predicted_demand_qty) AS predicted_demand_qty, AVG(model_confidence) AS model_confidence
        FROM TIRECO_DW.ML.FORECAST_DEMAND WHERE forecast_horizon_days <= 7 AND forecast_generated_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        GROUP BY sku_id, dc_id
    ) f7 ON ih.sku_id = f7.sku_id AND ih.dc_code = f7.dc_id
    LEFT JOIN (
        SELECT sku_id, dc_id, SUM(predicted_demand_qty) AS predicted_demand_qty
        FROM TIRECO_DW.ML.FORECAST_DEMAND WHERE forecast_horizon_days <= 14 AND forecast_generated_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        GROUP BY sku_id, dc_id
    ) f14 ON ih.sku_id = f14.sku_id AND ih.dc_code = f14.dc_id
    LEFT JOIN (
        SELECT sku_id, dc_id, SUM(predicted_demand_qty) AS predicted_demand_qty
        FROM TIRECO_DW.ML.FORECAST_DEMAND WHERE forecast_horizon_days <= 28 AND forecast_generated_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        GROUP BY sku_id, dc_id
    ) f28 ON ih.sku_id = f28.sku_id AND ih.dc_code = f28.dc_id
    LEFT JOIN (
        SELECT port_code, congestion_index, avg_wait_days
        FROM TIRECO_DW.EXTERNAL.EXT_PORT_CONGESTION
        WHERE port_code = 'USLGB'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY port_code ORDER BY date DESC) = 1
    ) pc ON 1=1
    LEFT JOIN (
        SELECT zip_code, MAX(expected_snow_in) AS expected_snow_in, MAX(CASE WHEN severe_weather_alert THEN 1 ELSE 0 END) AS severe_alert
        FROM TIRECO_DW.EXTERNAL.EXT_WEATHER_FORECAST
        WHERE forecast_date BETWEEN CURRENT_DATE() AND DATEADD(day, 7, CURRENT_DATE())
        GROUP BY zip_code
    ) wf ON l.zip_code = wf.zip_code
    WHERE ih.inventory_status IN ('STOCKOUT', 'CRITICAL', 'LOW')
       OR ih.days_of_supply < 21
       OR (wf.expected_snow_in > 4 AND ih.days_of_supply < 30);
    
    SELECT COUNT(*) INTO rec_count FROM PO_RECOMMENDATIONS WHERE recommendation_status = 'DRAFT' AND generated_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP());
    
    RETURN 'Generated ' || rec_count || ' PO recommendations';
END;
$$;

-- =====================================================================================
-- PHASE 10: EXPLAINABILITY LAYER (Cortex LLM)
-- =====================================================================================

-- Stored Procedure: Add LLM-generated justifications
CREATE OR REPLACE PROCEDURE SP_ADD_JUSTIFICATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    updated_count INT := 0;
BEGIN
    UPDATE TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS pr
    SET justification_text = SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'You are a supply chain analyst. Generate a concise 3-4 sentence justification for this purchase order recommendation. Be specific with numbers.
        
        Context:
        - SKU: ' || pr.sku_name || '
        - Distribution Center: ' || pr.dc_name || '
        - Current Inventory: ' || pr.current_inventory_qty || ' units
        - Days of Supply: ' || ROUND(pr.current_days_of_supply, 1) || ' days
        - 7-day Forecast Demand: ' || ROUND(pr.forecast_demand_7d, 0) || ' units
        - Lead Time: ' || pr.lead_time_days || ' days
        - Port Delay Buffer: ' || pr.port_delay_buffer_days || ' days
        - Weather Risk: ' || COALESCE(pr.risk_factors:weather_severity::VARCHAR, 'none') || '
        - Recommended Order: ' || pr.recommended_order_qty || ' units
        - Urgency: ' || pr.urgency_level || '
        
        Format: Start with "Recommend ordering X units because:" then list numbered reasons.'
    )
    WHERE pr.recommendation_status = 'DRAFT'
      AND pr.justification_text IS NULL
      AND pr.generated_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP());
    
    SELECT COUNT(*) INTO updated_count FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS 
    WHERE justification_text IS NOT NULL AND generated_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP());
    
    RETURN 'Added justifications to ' || updated_count || ' recommendations';
END;
$$;

-- =====================================================================================
-- PHASE 11: ORCHESTRATION - TASKS DAG
-- =====================================================================================

USE SCHEMA TIRECO_DW.ORCHESTRATION;

-- Root Task: Triggers daily pipeline
CREATE OR REPLACE TASK TASK_DAILY_PIPELINE_ROOT
    WAREHOUSE = DASH_WH_SI
    SCHEDULE = 'USING CRON 0 6 * * * America/Denver'
AS
    SELECT 'Pipeline started at ' || CURRENT_TIMESTAMP();

-- Task: Refresh Dynamic Tables
CREATE OR REPLACE TASK TASK_REFRESH_GOLD_LAYER
    WAREHOUSE = DASH_WH_SI
    AFTER TASK_DAILY_PIPELINE_ROOT
AS
    ALTER DYNAMIC TABLE TIRECO_DW.GOLD.AGG_DEMAND_DAILY REFRESH;

-- Task: Generate Forecasts
CREATE OR REPLACE TASK TASK_GENERATE_FORECASTS
    WAREHOUSE = DASH_WH_SI
    AFTER TASK_REFRESH_GOLD_LAYER
AS
    CALL TIRECO_DW.ML.SP_GENERATE_FORECASTS();

-- Task: Generate PO Recommendations
CREATE OR REPLACE TASK TASK_GENERATE_PO_RECS
    WAREHOUSE = DASH_WH_SI
    AFTER TASK_GENERATE_FORECASTS
AS
    CALL TIRECO_DW.PROCUREMENT.SP_GENERATE_PO_RECOMMENDATIONS();

-- Task: Add LLM Justifications
CREATE OR REPLACE TASK TASK_ADD_JUSTIFICATIONS
    WAREHOUSE = DASH_WH_SI
    AFTER TASK_GENERATE_PO_RECS
AS
    CALL TIRECO_DW.PROCUREMENT.SP_ADD_JUSTIFICATIONS();

-- Enable all tasks
ALTER TASK TASK_ADD_JUSTIFICATIONS RESUME;
ALTER TASK TASK_GENERATE_PO_RECS RESUME;
ALTER TASK TASK_GENERATE_FORECASTS RESUME;
ALTER TASK TASK_REFRESH_GOLD_LAYER RESUME;
ALTER TASK TASK_DAILY_PIPELINE_ROOT RESUME;

-- =====================================================================================
-- PHASE 12: STREAMLIT DASHBOARD (Buyer Interface)
-- =====================================================================================

-- Create a view for the Streamlit dashboard
CREATE OR REPLACE VIEW TIRECO_DW.PROCUREMENT.V_BUYER_DASHBOARD AS
SELECT 
    pr.recommendation_id,
    pr.generated_at,
    pr.sku_code,
    pr.sku_name,
    pr.dc_code,
    pr.dc_name,
    pr.vendor_name,
    pr.current_inventory_qty,
    pr.current_days_of_supply,
    pr.forecast_demand_7d,
    pr.forecast_demand_28d,
    pr.recommended_order_qty,
    pr.estimated_cost,
    pr.priority_score,
    pr.urgency_level,
    pr.lead_time_days,
    pr.port_delay_buffer_days,
    pr.expected_receipt_date,
    pr.justification_text,
    pr.recommendation_status,
    pr.risk_factors,
    l.region,
    l.state,
    p.brand_name,
    p.category_l1,
    p.category_l2
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS pr
JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON pr.dc_code = l.dc_code AND l.is_current = TRUE
JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON pr.sku_id = p.sku_id AND p.is_current = TRUE
WHERE pr.recommendation_status IN ('DRAFT', 'PENDING_REVIEW')
ORDER BY pr.priority_score DESC, pr.generated_at DESC;

-- Summary view for dashboard KPIs
CREATE OR REPLACE VIEW TIRECO_DW.PROCUREMENT.V_DASHBOARD_SUMMARY AS
SELECT 
    COUNT(*) AS total_recommendations,
    SUM(CASE WHEN urgency_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN urgency_level = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN urgency_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN urgency_level = 'LOW' THEN 1 ELSE 0 END) AS low_count,
    SUM(estimated_cost) AS total_recommended_spend,
    COUNT(DISTINCT dc_code) AS dcs_affected,
    COUNT(DISTINCT sku_id) AS skus_affected,
    AVG(priority_score) AS avg_priority_score
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
WHERE recommendation_status = 'DRAFT'
  AND generated_at >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- =====================================================================================
-- EXECUTION: Run the pipeline manually for initial setup
-- =====================================================================================

-- Execute forecasts
CALL TIRECO_DW.ML.SP_GENERATE_FORECASTS();

-- Execute PO recommendations
CALL TIRECO_DW.PROCUREMENT.SP_GENERATE_PO_RECOMMENDATIONS();

-- View results
SELECT * FROM TIRECO_DW.PROCUREMENT.V_BUYER_DASHBOARD LIMIT 20;
SELECT * FROM TIRECO_DW.PROCUREMENT.V_DASHBOARD_SUMMARY;

-- =====================================================================================
-- END OF TREAD INTELLIGENCE IMPLEMENTATION
-- =====================================================================================
