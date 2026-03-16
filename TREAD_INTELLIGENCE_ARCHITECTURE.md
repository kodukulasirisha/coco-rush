# TREAD INTELLIGENCE
## Intelligent Inventory Forecasting & Procurement System for Tireco

---

**Document Version:** 1.0  
**Date:** March 16, 2026  
**Author:** Cortex Code  
**Status:** Implementation Ready

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Enterprise Architecture](#4-enterprise-architecture)
5. [Data Model Design](#5-data-model-design)
6. [End-to-End Data Flow](#6-end-to-end-data-flow)
7. [Component Details](#7-component-details)
8. [Implementation Phases](#8-implementation-phases)
9. [Success Metrics](#9-success-metrics)
10. [Appendix](#10-appendix)

---

## 1. Executive Summary

### 1.1 Vision

Tread Intelligence transforms Tireco's supply chain from reactive to predictive by placing Cortex Code at the heart of inventory management. The system converts Snowflake from a passive data warehouse into an intelligent procurement engine that automatically generates purchase order recommendations with AI-powered justifications.

### 1.2 Key Outcomes

| Metric | Target Impact |
|--------|---------------|
| Manual forecasting time | **-70%** reduction |
| Stockout rate (high-demand SKUs) | **-50%** reduction |
| Overstock carrying costs | **-15%** reduction |
| Forecast accuracy (MAPE) | **<15%** |
| Auto-generated PO approval rate | **>80%** |

### 1.3 Technology Stack

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TECHNOLOGY COMPONENTS                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │  Snowflake  │  │  Cortex ML  │  │ Cortex LLM  │  │  Streamlit  │   │
│  │  Data Cloud │  │  Forecasting│  │  Complete   │  │  Dashboard  │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │   Dynamic   │  │  Snowflake  │  │   Cortex    │  │    dbt      │   │
│  │   Tables    │  │    Tasks    │  │   Analyst   │  │  (optional) │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Problem Statement

### 2.1 Current Challenges

Tireco faces four critical challenges in managing inventory across 30+ distribution centers:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            CORE BUSINESS PROBLEMS                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────────────────┐          ┌──────────────────────────┐            │
│  │    1. REACTIVE LAG       │          │  2. CONTEXTUAL BLINDNESS │            │
│  ├──────────────────────────┤          ├──────────────────────────┤            │
│  │ • Purchasing teams       │          │ • Internal PO data       │            │
│  │   respond AFTER demand   │          │   exists in vacuum       │            │
│  │   shifts occur           │          │ • Ignores weather,       │            │
│  │ • Yesterday's sales      │          │   port delays, economic  │            │
│  │   dictate today's orders │          │   signals                │            │
│  │ • Missed demand spikes   │          │ • No external context    │            │
│  └──────────────────────────┘          └──────────────────────────┘            │
│                                                                                  │
│  ┌──────────────────────────┐          ┌──────────────────────────┐            │
│  │  3. OPERATIONAL FRICTION │          │  4. NO PREDICTIVE MODELS │            │
│  ├──────────────────────────┤          ├──────────────────────────┤            │
│  │ • Skilled buyers stuck   │          │ • Manual calculations    │            │
│  │   running manual reports │          │   at granular level      │            │
│  │ • Thousands of hours on  │          │ • Hundreds of brands     │            │
│  │   data "grunt work"      │          │   across 30+ DCs         │            │
│  │ • No time for strategic  │          │ • Zip-code level         │            │
│  │   procurement            │          │   complexity impossible  │            │
│  └──────────────────────────┘          └──────────────────────────┘            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Business Impact of Current State

| Problem | Business Impact | Annual Cost |
|---------|-----------------|-------------|
| Reactive Lag | Missed sales, emergency freight | $2-5M |
| Contextual Blindness | Stockouts during weather events | $1-3M |
| Operational Friction | Buyer productivity at 40% | $500K+ |
| No Predictive Models | 20% excess inventory | $3-6M |

---

## 3. Solution Overview

### 3.1 The Tread Intelligence Approach

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SOLUTION: TREAD INTELLIGENCE                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│                    FROM REACTIVE ────────────► TO PREDICTIVE                    │
│                                                                                  │
│  ┌────────────────────────┐              ┌────────────────────────┐            │
│  │      BEFORE            │              │       AFTER            │            │
│  │                        │              │                        │            │
│  │  Yesterday's Sales     │              │  Tomorrow's Weather    │            │
│  │         ↓              │              │  + Port Delays         │            │
│  │  Manual Spreadsheets   │              │  + Economic Signals    │            │
│  │         ↓              │              │         ↓              │            │
│  │  Today's PO Decision   │              │  AI Forecasts          │            │
│  │         ↓              │              │         ↓              │            │
│  │  Hope for the Best     │              │  Auto PO Recs + Why    │            │
│  │                        │              │         ↓              │            │
│  │                        │              │  Buyer Validates       │            │
│  └────────────────────────┘              └────────────────────────┘            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Core Capabilities

| Capability | Description | Snowflake Feature |
|------------|-------------|-------------------|
| **Data Enrichment** | Blend internal data with external signals | Marketplace + Dynamic Tables |
| **Multi-Grain Forecasting** | Predict at SKU-DC-Day level | Cortex ML FORECAST |
| **Auto PO Generation** | Calculate optimal order quantities | Stored Procedures |
| **Explainable AI** | Human-readable justifications | Cortex LLM Complete |
| **Orchestration** | Automated daily pipeline | Snowflake Tasks |

---

## 4. Enterprise Architecture

### 4.1 High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              TREAD INTELLIGENCE ARCHITECTURE                                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌─────────────────────┐
                                    │   DELIVERY LAYER    │
                                    │                     │
                                    │  ┌───────────────┐  │
                                    │  │   Streamlit   │  │
                                    │  │   Dashboard   │  │
                                    │  └───────┬───────┘  │
                                    │          │          │
                                    │  ┌───────┴───────┐  │
                                    │  │    Cortex     │  │
                                    │  │    Analyst    │  │
                                    │  └───────────────┘  │
                                    └──────────┬──────────┘
                                               │
                    ┌──────────────────────────┴──────────────────────────┐
                    │                  INTELLIGENCE LAYER                  │
                    │                                                      │
                    │  ┌─────────────────┐      ┌─────────────────┐       │
                    │  │  PO RECOMMEND   │      │   CORTEX LLM    │       │
                    │  │     ENGINE      │─────►│  Explainability │       │
                    │  └────────┬────────┘      └─────────────────┘       │
                    │           │                                          │
                    │  ┌────────┴────────┐                                │
                    │  │   CORTEX ML     │                                │
                    │  │   Forecasting   │                                │
                    │  └────────┬────────┘                                │
                    └───────────┼──────────────────────────────────────────┘
                                │
                    ┌───────────┴──────────────────────────────────────────┐
                    │                    GOLD LAYER                         │
                    │              (Dynamic Tables - Hourly)                │
                    │                                                       │
                    │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐    │
                    │  │AGG_DEMAND_  │ │AGG_DEMAND_  │ │ INVENTORY_  │    │
                    │  │   DAILY     │ │   WEEKLY    │ │   HEALTH    │    │
                    │  │(ML Features)│ │  (Trends)   │ │  (Current)  │    │
                    │  └─────────────┘ └─────────────┘ └─────────────┘    │
                    └───────────────────────┬──────────────────────────────┘
                                            │
          ┌─────────────────────────────────┴─────────────────────────────────┐
          │                         SILVER LAYER                               │
          │                    (Conformed Dimensions & Facts)                  │
          │                                                                    │
          │  ┌──────────────────────────────┐ ┌──────────────────────────────┐│
          │  │        DIMENSIONS            │ │           FACTS              ││
          │  │ ┌─────────┐ ┌─────────┐     │ │ ┌─────────┐ ┌─────────┐     ││
          │  │ │DIM_DATE │ │DIM_     │     │ │ │FACT_    │ │FACT_    │     ││
          │  │ │         │ │PRODUCT  │     │ │ │SALES_   │ │INVENTORY│     ││
          │  │ └─────────┘ └─────────┘     │ │ │ORDERS   │ │_SNAPSHOT│     ││
          │  │ ┌─────────┐ ┌─────────┐     │ │ └─────────┘ └─────────┘     ││
          │  │ │DIM_     │ │DIM_     │     │ │ ┌─────────┐                 ││
          │  │ │LOCATION │ │VENDOR   │     │ │ │FACT_    │                 ││
          │  │ └─────────┘ └─────────┘     │ │ │PURCHASE_│                 ││
          │  │ ┌─────────┐                 │ │ │ORDERS   │                 ││
          │  │ │DIM_     │                 │ │ └─────────┘                 ││
          │  │ │CUSTOMER │                 │ │                             ││
          │  │ └─────────┘                 │ │                             ││
          │  └──────────────────────────────┘ └──────────────────────────────┘│
          └───────────────────────────────┬────────────────────────────────────┘
                                          │
    ┌─────────────────────────────────────┴─────────────────────────────────────┐
    │                              BRONZE LAYER                                  │
    │                         (Raw Source Replication)                           │
    │                                                                            │
    │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐  │
    │  │    ERP    │ │    WMS    │ │    TMS    │ │    CRM    │ │    MDM    │  │
    │  │ (15 min)  │ │(Real-time)│ │ (Hourly)  │ │ (Hourly)  │ │ (Daily)   │  │
    │  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘  │
    │        │             │             │             │             │         │
    │  ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐  │
    │  │ Purchase  │ │ Inventory │ │ Shipments │ │ Customers │ │ Products  │  │
    │  │ Orders    │ │ Snapshot  │ │ Carriers  │ │ Addresses │ │ Tire Specs│  │
    │  │ Vendors   │ │ Inv Txns  │ │ Freight   │ │           │ │ Hierarchy │  │
    │  │ Receipts  │ │ Bin Locs  │ │ Rates     │ │           │ │ Fitment   │  │
    │  └───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘  │
    └───────────────────────────────────┬──────────────────────────────────────┘
                                        │
                                        │
    ┌───────────────────────────────────┴──────────────────────────────────────┐
    │                           EXTERNAL LAYER                                  │
    │                     (Snowflake Marketplace Data)                          │
    │                                                                           │
    │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ │
    │  │  Weather  │ │  Weather  │ │   Port    │ │ Economic  │ │  Vehicle  │ │
    │  │Historical │ │ Forecast  │ │Congestion │ │Indicators │ │Registr.   │ │
    │  │ (by zip)  │ │ (14-day)  │ │ (7 ports) │ │ (by state)│ │ (by zip)  │ │
    │  └───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘ │
    └──────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Source Systems Landscape

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              TIRECO SOURCE SYSTEMS LANDSCAPE                                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│      ERP        │  │      WMS        │  │      TMS        │  │      CRM        │  │      MDM        │
│   (SAP/Oracle)  │  │  (Manhattan)    │  │  (Blue Yonder)  │  │  (Salesforce)   │  │   (Master)      │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ • Purchase Ords │  │ • Bin Locations │  │ • Shipments     │  │ • Accounts      │  │ • Products      │
│ • Vendors       │  │ • Inventory Txn │  │ • Carriers      │  │ • Contacts      │  │ • Tire Specs    │
│ • AP/AR         │  │ • Cycle Counts  │  │ • Freight Rates │  │ • Opportunities │  │ • Hierarchy     │
│ • GL Entries    │  │ • Receipts      │  │ • Routes        │  │ • Cases         │  │ • Fitment       │
│ • Vendor Terms  │  │ • Picks/Packs   │  │ • Delivery Appt │  │ • Leads         │  │ • Substitutes   │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    SUPPLIER     │  │     SALES       │  │    PRICING      │  │    QUALITY      │
│    PORTAL       │  │    SYSTEM       │  │    ENGINE       │  │    SYSTEM       │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ • ASN (Advance  │  │ • Sales Orders  │  │ • List Prices   │  │ • Inspections   │
│   Ship Notice)  │  │ • Order Lines   │  │ • Cost Basis    │  │ • Defect Codes  │
│ • Lead Times    │  │ • Returns       │  │ • Promotions    │  │ • Warranties    │
│ • Capacity      │  │ • Backorders    │  │ • Contracts     │  │ • Claims        │
│ • Scorecards    │  │ • Allocations   │  │ • Rebates       │  │ • RMA           │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘
```

---

## 5. Data Model Design

### 5.1 Database Schema Structure

```
TIRECO_DW (Database)
├── BRONZE      - Raw source system replicas
├── EXTERNAL    - Marketplace data (Weather, Port, Economic)
├── SILVER      - Cleansed dimensions and facts
├── GOLD        - Aggregations and ML features
├── ML          - Forecasting models and outputs
├── PROCUREMENT - PO recommendations
└── ORCHESTRATION - Tasks and procedures
```

### 5.2 Bronze Layer Tables (50+ Tables)

#### ERP System Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `RAW_ERP_PURCHASE_ORDERS` | PO headers | po_id, vendor_id, dc_id, order_date, expected_delivery |
| `RAW_ERP_PO_LINE_ITEMS` | PO line items | po_line_id, sku_id, ordered_qty, unit_cost |
| `RAW_ERP_PO_RECEIPTS` | Receiving transactions | receipt_id, received_qty, received_date, quality_status |
| `RAW_ERP_VENDORS` | Vendor master | vendor_id, vendor_name, country_code, risk_rating |
| `RAW_ERP_VENDOR_TERMS` | Lead times, MOQs | term_id, lead_time_days, min_order_qty, order_multiple |
| `RAW_ERP_AP_INVOICES` | Accounts payable | invoice_id, invoice_amount, payment_status |

#### WMS System Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `RAW_WMS_DISTRIBUTION_CENTERS` | DC master | dc_id, dc_name, city, state, zip_code, lat, lon |
| `RAW_WMS_INVENTORY` | Current inventory | sku_id, dc_id, qty_on_hand, qty_available, qty_in_transit |
| `RAW_WMS_INVENTORY_TRANSACTIONS` | Inventory movements | txn_id, txn_type, quantity, txn_timestamp |
| `RAW_WMS_BIN_LOCATIONS` | Bin master | bin_id, dc_id, zone_code, velocity_class |
| `RAW_WMS_CYCLE_COUNTS` | Cycle count results | count_id, system_qty, counted_qty, variance |

#### TMS System Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `RAW_TMS_SHIPMENTS` | Outbound shipments | shipment_id, carrier_id, ship_date, freight_cost |
| `RAW_TMS_CARRIERS` | Carrier master | carrier_id, carrier_name, scac_code, safety_rating |
| `RAW_TMS_FREIGHT_RATES` | Rate tables | rate_id, origin_zone, dest_zone, rate_per_cwt |

#### MDM (Product) Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `RAW_MDM_PRODUCTS` | SKU master | sku_id, sku_code, sku_name, primary_vendor_id |
| `RAW_MDM_PRODUCT_HIERARCHY` | Brand/category | brand_name, category_l1, category_l2, price_tier |
| `RAW_MDM_TIRE_SPECS` | Tire specifications | tire_width, aspect_ratio, rim_diameter, treadwear_rating |
| `RAW_MDM_VEHICLE_FITMENT` | Vehicle compatibility | vehicle_make, vehicle_model, vehicle_year, oem_tire_size |

#### Sales & CRM Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `RAW_SALES_ORDERS` | Sales order headers | order_id, customer_id, dc_id, order_date, order_source |
| `RAW_SALES_ORDER_LINES` | Order line items | order_line_id, sku_id, quantity, unit_price |
| `RAW_SALES_RETURNS` | Returns/RMAs | return_id, return_qty, return_reason |
| `RAW_CRM_CUSTOMERS` | Customer master | customer_id, customer_type, tier_level, credit_limit |

### 5.3 External Data Tables (Marketplace)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL DATA SOURCES (MARKETPLACE)                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  WEATHER DATA                                                                                      │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  EXT_WEATHER_HISTORICAL (by zip code, daily)    │  EXT_WEATHER_FORECAST (14-day lookahead)       │
│  • date, zip_code, city, state                  │  • forecast_date, zip_code                     │
│  • temp_high_f, temp_low_f                      │  • temp_high_f, temp_low_f                     │
│  • precipitation_in, snowfall_in               │  • precip_probability_pct                      │
│  • weather_severity_index (1-5)                │  • snow_probability_pct, expected_snow_in      │
│  • condition_code (CLEAR/RAIN/SNOW/etc)        │  • severe_weather_alert (boolean)              │
│                                                 │  • forecast_confidence                         │
│                                                                                                    │
│  USE CASE: Winter tire demand spikes with snowfall predictions                                    │
│            Regional inventory pre-positioning before storms                                        │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PORT & LOGISTICS DATA                                                                             │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  EXT_PORT_CONGESTION (7 major ports, daily)    │  COVERED PORTS:                                 │
│  • port_code, port_name, date                  │  • USLGB - Port of Long Beach                  │
│  • vessels_at_anchor                           │  • USLAX - Port of Los Angeles                 │
│  • avg_wait_days                               │  • USSEA - Port of Seattle                     │
│  • container_dwell_time_days                   │  • USOAK - Port of Oakland                     │
│  • throughput_teu                              │  • USNYC - Port of New York/NJ                 │
│  • congestion_index (1-100)                    │  • USSAV - Port of Savannah                    │
│  • delay_trend (INCREASING/STABLE/DECREASING)  │  • USHOU - Port of Houston                     │
│                                                                                                    │
│  USE CASE: Adjust safety stock buffers when port delays exceed thresholds                         │
│            Extend lead times dynamically based on congestion                                       │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  ECONOMIC & MARKET DATA                                                                            │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  EXT_ECONOMIC_INDICATORS (monthly, by state)   │  EXT_VEHICLE_REGISTRATIONS (monthly, by zip)   │
│  • consumer_confidence_index                   │  • vehicle_segment (Passenger/Truck/SUV)       │
│  • unemployment_rate                           │  • registered_count                            │
│  • gas_price_avg                               │  • avg_vehicle_age                             │
│  • inflation_rate                              │  • pct_trucks_suvs                             │
│  • vehicle_sales_index                         │                                                 │
│                                                                                                    │
│  USE CASE: Demand elasticity modeling, replacement tire demand correlation                        │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.4 Silver Layer - Dimensions

#### DIM_DATE
```sql
┌──────────────────────────────────────────────────────────────────┐
│                          DIM_DATE                                │
├──────────────────────────────────────────────────────────────────┤
│ date_sk              INT (PK)      -- YYYYMMDD format           │
│ date_actual          DATE          -- Actual date               │
│ day_of_week          INT           -- 0-6                       │
│ day_of_week_name     VARCHAR       -- Monday, Tuesday...        │
│ week_of_year         INT           -- 1-53                      │
│ month_actual         INT           -- 1-12                      │
│ month_name           VARCHAR       -- January, February...      │
│ quarter_actual       INT           -- 1-4                       │
│ year_actual          INT           -- 2024, 2025...             │
│ is_weekend           BOOLEAN                                     │
│ is_holiday           BOOLEAN                                     │
│ season               VARCHAR       -- WINTER/SPRING/SUMMER/FALL │
│ fiscal_week          INT                                         │
│ fiscal_year          INT                                         │
└──────────────────────────────────────────────────────────────────┘
```

#### DIM_PRODUCT
```sql
┌──────────────────────────────────────────────────────────────────┐
│                         DIM_PRODUCT                              │
├──────────────────────────────────────────────────────────────────┤
│ product_sk           INT (PK)      -- Surrogate key             │
│ sku_id               VARCHAR       -- Natural key               │
│ sku_code             VARCHAR       -- MST-285/70R17-MT          │
│ sku_name             VARCHAR       -- Milestar Patagonia M/T... │
│ brand_name           VARCHAR       -- Milestar, Falken, Nitto   │
│ sub_brand            VARCHAR       -- Patagonia, Wildpeak       │
│ category_l1          VARCHAR       -- PASSENGER/LIGHT_TRUCK     │
│ category_l2          VARCHAR       -- MUD_TERRAIN/ALL_TERRAIN   │
│ category_l3          VARCHAR       -- OFF_ROAD/ADVENTURE        │
│ tire_width           INT           -- 285                       │
│ aspect_ratio         INT           -- 70                        │
│ rim_diameter         INT           -- 17                        │
│ load_index           INT           -- 121                       │
│ speed_rating         VARCHAR       -- S, H, V, W                │
│ tire_type            VARCHAR       -- MUD_TERRAIN               │
│ treadwear_rating     INT           -- 40-700                    │
│ warranty_miles       INT           -- 50000                     │
│ price_tier           VARCHAR       -- PREMIUM/MID_RANGE/VALUE   │
│ primary_vendor_id    VARCHAR                                     │
│ unit_weight_lbs      DECIMAL                                     │
│ is_active            BOOLEAN                                     │
│ valid_from           TIMESTAMP     -- SCD Type 2                │
│ valid_to             TIMESTAMP                                   │
│ is_current           BOOLEAN                                     │
└──────────────────────────────────────────────────────────────────┘
```

#### DIM_LOCATION
```sql
┌──────────────────────────────────────────────────────────────────┐
│                         DIM_LOCATION                             │
├──────────────────────────────────────────────────────────────────┤
│ location_sk          INT (PK)      -- Surrogate key             │
│ location_id          VARCHAR       -- DC001                     │
│ location_type        VARCHAR       -- DC                        │
│ dc_code              VARCHAR       -- DC001                     │
│ dc_name              VARCHAR       -- Denver Distribution Center│
│ city                 VARCHAR       -- Denver                    │
│ state                VARCHAR       -- CO                        │
│ zip_code             VARCHAR       -- 80202                     │
│ zip_3                VARCHAR       -- 802                       │
│ region               VARCHAR       -- WEST/MIDWEST/EAST/SOUTH   │
│ lat                  DECIMAL       -- 39.7392                   │
│ lon                  DECIMAL       -- -104.9903                 │
│ timezone             VARCHAR       -- America/Denver            │
│ climate_zone         VARCHAR       -- COLD/HOT/TEMPERATE/WET    │
│ total_sqft           INT           -- 150000                    │
│ manager_name         VARCHAR                                     │
│ is_active            BOOLEAN                                     │
│ valid_from           TIMESTAMP                                   │
│ valid_to             TIMESTAMP                                   │
│ is_current           BOOLEAN                                     │
└──────────────────────────────────────────────────────────────────┘
```

#### DIM_VENDOR
```sql
┌──────────────────────────────────────────────────────────────────┐
│                          DIM_VENDOR                              │
├──────────────────────────────────────────────────────────────────┤
│ vendor_sk            INT (PK)                                    │
│ vendor_id            VARCHAR       -- V001                      │
│ vendor_code          VARCHAR       -- TIRECO                    │
│ vendor_name          VARCHAR       -- Tireco Inc.               │
│ vendor_type          VARCHAR       -- MANUFACTURER              │
│ country_code         VARCHAR       -- USA/CHN/JPN/TWN           │
│ risk_rating          VARCHAR       -- LOW/MEDIUM/HIGH           │
│ avg_lead_time_days   INT           -- 14, 28, 35                │
│ min_order_qty        INT           -- 100                       │
│ on_time_delivery_pct DECIMAL       -- 95.5                      │
│ quality_score        DECIMAL       -- 98.2                      │
│ overall_score        DECIMAL       -- 94.8                      │
│ is_active            BOOLEAN                                     │
│ valid_from           TIMESTAMP                                   │
│ valid_to             TIMESTAMP                                   │
│ is_current           BOOLEAN                                     │
└──────────────────────────────────────────────────────────────────┘
```

### 5.5 Silver Layer - Facts

#### FACT_SALES_ORDERS
```sql
┌──────────────────────────────────────────────────────────────────┐
│                       FACT_SALES_ORDERS                          │
├──────────────────────────────────────────────────────────────────┤
│ sales_order_sk       INT (PK)                                    │
│ order_line_id        VARCHAR                                     │
│ order_date_sk        INT (FK → DIM_DATE)                        │
│ product_sk           INT (FK → DIM_PRODUCT)                     │
│ customer_sk          INT (FK → DIM_CUSTOMER)                    │
│ ship_from_location_sk INT (FK → DIM_LOCATION)                   │
│ order_source         VARCHAR       -- WEB/EDI/PHONE/B2B         │
│ ordered_qty          INT                                         │
│ unit_price           DECIMAL                                     │
│ discount_amount      DECIMAL                                     │
│ extended_price       DECIMAL                                     │
│ unit_cost            DECIMAL                                     │
│ gross_margin         DECIMAL                                     │
│ line_status          VARCHAR                                     │
└──────────────────────────────────────────────────────────────────┘
```

#### FACT_INVENTORY_SNAPSHOT
```sql
┌──────────────────────────────────────────────────────────────────┐
│                    FACT_INVENTORY_SNAPSHOT                       │
├──────────────────────────────────────────────────────────────────┤
│ inventory_snapshot_sk INT (PK)                                   │
│ snapshot_date_sk     INT (FK → DIM_DATE)                        │
│ product_sk           INT (FK → DIM_PRODUCT)                     │
│ location_sk          INT (FK → DIM_LOCATION)                    │
│ qty_on_hand          INT                                         │
│ qty_allocated        INT                                         │
│ qty_available        INT                                         │
│ qty_in_transit       INT                                         │
│ qty_on_order         INT                                         │
│ unit_cost            DECIMAL                                     │
│ extended_cost        DECIMAL                                     │
└──────────────────────────────────────────────────────────────────┘
```

#### FACT_PURCHASE_ORDERS
```sql
┌──────────────────────────────────────────────────────────────────┐
│                     FACT_PURCHASE_ORDERS                         │
├──────────────────────────────────────────────────────────────────┤
│ purchase_order_sk    INT (PK)                                    │
│ po_line_id           VARCHAR                                     │
│ order_date_sk        INT (FK → DIM_DATE)                        │
│ expected_delivery_sk INT (FK → DIM_DATE)                        │
│ product_sk           INT (FK → DIM_PRODUCT)                     │
│ vendor_sk            INT (FK → DIM_VENDOR)                      │
│ location_sk          INT (FK → DIM_LOCATION)                    │
│ ordered_qty          INT                                         │
│ received_qty         INT                                         │
│ unit_cost            DECIMAL                                     │
│ extended_cost        DECIMAL                                     │
│ line_status          VARCHAR                                     │
│ lead_time_actual_days INT                                        │
└──────────────────────────────────────────────────────────────────┘
```

### 5.6 Gold Layer - Dynamic Tables

#### AGG_DEMAND_DAILY (Primary ML Feature Table)

```sql
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           AGG_DEMAND_DAILY (Dynamic Table - Hourly Refresh)                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│  GRAIN: Date + SKU + DC                                                                            │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ KEY DIMENSIONS                                                                              │  │
│  │ • date_actual, sku_id, sku_code, dc_id, dc_code                                            │  │
│  │ • brand_name, category_l1, category_l2, tire_type, price_tier                              │  │
│  │ • city, state, zip_code, region, climate_zone                                              │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ TIME ATTRIBUTES                                                                             │  │
│  │ • day_of_week, day_of_week_name, is_weekend                                                │  │
│  │ • week_of_year, month_actual, quarter_actual, year_actual, season                          │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ INTERNAL SIGNALS (Measures)                                                                 │  │
│  │ • sales_qty, sales_revenue, sales_orders_count                                             │  │
│  │ • avg_unit_price, avg_gross_margin                                                         │  │
│  │ • inventory_eod, qty_in_transit, qty_on_order                                              │  │
│  │ • days_of_supply, stockout_flag                                                            │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ EXTERNAL SIGNALS (Marketplace Enriched)                                                     │  │
│  │ • weather_temp_high_f, weather_temp_low_f                                                  │  │
│  │ • weather_precip_in, weather_snow_in                                                       │  │
│  │ • weather_severity_index, weather_condition_code                                           │  │
│  │ • port_delay_index, port_avg_wait_days                                                     │  │
│  │ • consumer_confidence_index, fuel_price_regional, vehicle_sales_index                      │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### INVENTORY_HEALTH_CURRENT

```sql
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                      INVENTORY_HEALTH_CURRENT (Dynamic Table - 30 min Refresh)                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  • sku_id, sku_code, sku_name, brand_name, category_l1, category_l2                              │
│  • dc_code, dc_name, city, state, region                                                          │
│  • qty_on_hand, qty_allocated, qty_available, qty_in_transit, qty_on_order                       │
│  • unit_cost, extended_cost                                                                       │
│  • avg_daily_sales_7d                                                                             │
│  • days_of_supply                                                                                 │
│  • primary_vendor, avg_lead_time_days, min_order_qty                                             │
│  • inventory_status: STOCKOUT | CRITICAL | LOW | HEALTHY | OVERSTOCK                             │
│  • last_updated                                                                                   │
│                                                                                                    │
│  STATUS LOGIC:                                                                                    │
│  • STOCKOUT: qty_available <= 0                                                                   │
│  • CRITICAL: days_of_supply < lead_time_days                                                      │
│  • LOW: days_of_supply < lead_time_days * 1.5                                                     │
│  • OVERSTOCK: days_of_supply > 60                                                                 │
│  • HEALTHY: otherwise                                                                             │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. End-to-End Data Flow

### 6.1 Complete Pipeline Visualization

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              TREAD INTELLIGENCE: COMPLETE DATA FLOW                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 1: DATA INGESTION (Snowpipe / Kafka / Fivetran)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
  │    ERP      │   │    WMS      │   │    TMS      │   │    CRM      │   │  SUPPLIER   │   │  PRICING    │
  │  (SAP/Ora)  │   │ (Manhattan) │   │(BlueYonder) │   │(Salesforce) │   │   PORTAL    │   │  ENGINE     │
  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
         │                 │                 │                 │                 │                 │
         │    CDC/Batch    │    CDC/Batch    │    CDC/Batch    │    API          │    EDI/API      │    API
         │    (15 min)     │    (Real-time)  │    (Hourly)     │    (Hourly)     │    (Daily)      │    (Daily)
         │                 │                 │                 │                 │                 │
         ▼                 ▼                 ▼                 ▼                 ▼                 ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                      SNOWFLAKE LANDING ZONE                                            │
  │                                     (RAW Stage / External Tables)                                      │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 2: BRONZE LAYER (Raw Persistence - Append-Only with Metadata)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                              TIRECO_DW.BRONZE                                                              │
  │  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
  │  │                                          50+ RAW TABLES                                                              │  │
  │  │  RAW_ERP_PO | RAW_WMS_INVENTORY | RAW_TMS_SHIPMENTS | RAW_SALES_ORDERS | RAW_MDM_PRODUCTS | ...                     │  │
  │  └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘  │
  │  METADATA COLUMNS: _loaded_at, _source_system, _batch_id                                                                  │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         │       ┌──────────────────────────────────────────────────────────────────────────────────────┐
         │       │                           TIRECO_DW.EXTERNAL (Marketplace)                           │
         │       │  EXT_WEATHER_HISTORICAL | EXT_WEATHER_FORECAST | EXT_PORT_CONGESTION | ...          │
         │       └──────────────────────────────────────────────────────────────────────────────────────┘
         │                                               │
         ▼                                               ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 3: SILVER LAYER (dbt Transformations - Cleanse, Conform, Integrate)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                              TIRECO_DW.SILVER                                                              │
  │                                                                                                                            │
  │  ┌───────────────────────────────────────────────┐  ┌───────────────────────────────────────────────┐                    │
  │  │           DIMENSIONS (SCD Type 2)             │  │              FACTS (Transactional)            │                    │
  │  │  DIM_DATE | DIM_PRODUCT | DIM_LOCATION       │  │  FACT_SALES_ORDERS | FACT_INVENTORY_SNAPSHOT  │                    │
  │  │  DIM_VENDOR | DIM_CUSTOMER                   │  │  FACT_PURCHASE_ORDERS                         │                    │
  │  └───────────────────────────────────────────────┘  └───────────────────────────────────────────────┘                    │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 4: GOLD LAYER (Dynamic Tables - Aggregations & Feature Engineering)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                              TIRECO_DW.GOLD                                                                │
  │                                                                                                                            │
  │   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
  │   │  AGG_DEMAND_DAILY (ML Features)                                                                                     │ │
  │   │  • Internal Signals: sales_qty, inventory, backorders                                                               │ │
  │   │  • External Signals: weather, port delays, economic indicators                                                      │ │
  │   │  • Lag Features: sales_lag_7d, sales_lag_28d, rolling averages                                                     │ │
  │   └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
  │   ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐                                              │
  │   │ AGG_DEMAND_WEEKLY   │  │ AGG_DEMAND_MONTHLY  │  │INVENTORY_HEALTH_NOW │                                              │
  │   └─────────────────────┘  └─────────────────────┘  └─────────────────────┘                                              │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 5: ML FORECASTING (Cortex ML)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                         TIRECO_DW.ML                                                                       │
  │                                                                                                                            │
  │  ┌──────────────────────────────────┐              ┌──────────────────────────────────┐                                   │
  │  │   CORTEX ML FORECAST             │              │   FORECAST_DEMAND (Output)       │                                   │
  │  │   • Time-series per SKU-DC       │─────────────►│   • predicted_demand_qty         │                                   │
  │  │   • Exogenous: weather, ports    │              │   • prediction_interval_95       │                                   │
  │  │   • Horizons: 1, 7, 14, 28 days  │              │   • model_confidence             │                                   │
  │  └──────────────────────────────────┘              └──────────────────────────────────┘                                   │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 6: PO RECOMMENDATION ENGINE
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                      SP_GENERATE_PO_RECOMMENDATIONS                                                        │
  │                                                                                                                            │
  │   INPUTS:                           CALCULATION:                        OUTPUT:                                           │
  │   ┌─────────────────────┐          ┌─────────────────────┐            ┌─────────────────────┐                            │
  │   │ Forecast Demand     │          │ Required_Qty =      │            │ PO_RECOMMENDATIONS  │                            │
  │   │ Current Inventory   │─────────►│   Forecast_28d      │───────────►│ • sku_id, dc_id     │                            │
  │   │ Vendor Lead Times   │          │   - Inventory       │            │ • recommended_qty   │                            │
  │   │ Port Delays         │          │   - In_Transit      │            │ • priority_score    │                            │
  │   │ Safety Stock Rules  │          │   + Safety_Stock    │            │ • urgency_level     │                            │
  │   └─────────────────────┘          │   + Port_Buffer     │            │ • estimated_cost    │                            │
  │                                    └─────────────────────┘            └─────────────────────┘                            │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 7: EXPLAINABILITY LAYER (Cortex LLM)
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                      SNOWFLAKE.CORTEX.COMPLETE()                                                           │
  │                                                                                                                            │
  │   INPUT (per recommendation):                    OUTPUT (justification_text):                                             │
  │   ┌────────────────────────────────┐            ┌──────────────────────────────────────────────────────────────────────┐ │
  │   │ • SKU: Milestar Patagonia M/T  │            │ "Recommend ordering 700 units of Milestar Patagonia M/T for         │ │
  │   │ • DC: Denver                   │            │  Denver DC because:                                                   │ │
  │   │ • Current Inventory: 45        │───────────►│                                                                       │ │
  │   │ • Days of Supply: 2.3          │            │  (1) WEATHER: Heavy snow forecast next 5 days                        │ │
  │   │ • Weather: Snow forecast       │            │  (2) INVENTORY: Current stock covers only 2.3 days                   │ │
  │   │ • Port Delay: +4 days          │            │  (3) SUPPLY CHAIN: Port delays extending lead times                  │ │
  │   └────────────────────────────────┘            │                                                                       │ │
  │                                                 │  RISK: Stockout by March 19 if not ordered today."                   │ │
  │                                                 └──────────────────────────────────────────────────────────────────────┘ │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 PHASE 8: DELIVERY & ACTION
═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                                                            │
  │   ┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐│
  │   │    STREAMLIT APP        │    │    ERP INTEGRATION      │    │    EMAIL/SLACK ALERTS   │    │   CORTEX ANALYST        ││
  │   │    (Buyer Dashboard)    │    │    (SAP/Oracle API)     │    │    (Critical Items)     │    │   (Natural Language)    ││
  │   │                         │    │                         │    │                         │    │                         ││
  │   │ • View recommendations  │    │ • Push approved POs     │    │ • CRITICAL urgency      │    │ "What should I order    ││
  │   │ • Approve/Reject/Modify │    │ • Status sync           │    │   alerts                │    │  for Denver this week?" ││
  │   │ • Bulk actions          │    │ • Receipt confirmation  │    │ • Daily digest          │    │                         ││
  │   └─────────────────────────┘    └─────────────────────────┘    └─────────────────────────┘    └─────────────────────────┘│
  │                                                                                                                            │
  └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Orchestration DAG

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    ORCHESTRATION: SNOWFLAKE TASKS DAG                                │
├──────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                      │
│     ┌─────────────────────┐                                                                         │
│     │  TASK_DAILY_ROOT    │──────────────────────────────────────────────────────────────────────┐  │
│     │  (6 AM Daily)       │                                                                      │  │
│     │  CRON Schedule      │                                                                      │  │
│     └──────────┬──────────┘                                                                      │  │
│                │                                                                                 │  │
│                ▼                                                                                 │  │
│     ┌─────────────────────┐                                                                      │  │
│     │ TASK_REFRESH_GOLD   │                                                                      │  │
│     │ ALTER DYNAMIC TABLE │                                                                      │  │
│     │ AGG_DEMAND_DAILY    │                                                                      │  │
│     │ REFRESH             │                                                                      │  │
│     └──────────┬──────────┘                                                                      │  │
│                │                                                                                 │  │
│                ▼                                                                                 │  │
│     ┌─────────────────────┐                                                                      │  │
│     │ TASK_GENERATE_      │                                                                      │  │
│     │ FORECASTS           │                                                                      │  │
│     │ CALL SP_GENERATE_   │                                                                      │  │
│     │ FORECASTS()         │                                                                      │  │
│     └──────────┬──────────┘                                                                      │  │
│                │                                                                                 │  │
│                ▼                                                                                 │  │
│     ┌─────────────────────┐                                                                      │  │
│     │ TASK_GENERATE_      │                                                                      │  │
│     │ PO_RECS             │                                                                      │  │
│     │ CALL SP_GENERATE_   │                                                                      │  │
│     │ PO_RECOMMENDATIONS()│                                                                      │  │
│     └──────────┬──────────┘                                                                      │  │
│                │                                                                                 │  │
│                ▼                                                                                 │  │
│     ┌─────────────────────┐                                                                      │  │
│     │ TASK_ADD_           │◄─────────────────────────────────────────────────────────────────────┘  │
│     │ JUSTIFICATIONS      │                                                                         │
│     │ CALL SP_ADD_        │                                                                         │
│     │ JUSTIFICATIONS()    │                                                                         │
│     └─────────────────────┘                                                                         │
│                                                                                                      │
│  SCHEDULE: 6 AM Daily | WAREHOUSE: DASH_WH_SI                                                       │
│                                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Component Details

### 7.1 PO Recommendation Calculation Logic

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              PO RECOMMENDATION CALCULATION LOGIC                                    │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  SAFETY_STOCK = Avg_Daily_Demand × 7 days                                                         │
│                 (1 week buffer for demand variability)                                             │
│                                                                                                    │
│  REORDER_POINT = (Avg_Daily_Demand × Lead_Time_Days) + Safety_Stock                               │
│                                                                                                    │
│  PORT_DELAY_BUFFER = IF (Port_Congestion_Index > 70)                                              │
│                      THEN Port_Avg_Wait_Days × Avg_Daily_Demand                                   │
│                      ELSE 0                                                                        │
│                                                                                                    │
│  ─────────────────────────────────────────────────────────────────────────────────────────────    │
│                                                                                                    │
│  REQUIRED_QTY = Forecasted_Demand_28d                                                             │
│               - Current_Inventory                                                                  │
│               - In_Transit_Qty                                                                     │
│               - Open_PO_Qty                                                                        │
│               + Safety_Stock                                                                       │
│               + Port_Delay_Buffer                                                                  │
│                                                                                                    │
│  ─────────────────────────────────────────────────────────────────────────────────────────────    │
│                                                                                                    │
│  IF REQUIRED_QTY > 0 AND                                                                          │
│     (Current_Inventory < Reorder_Point OR Days_Of_Supply < Target_DOS)                            │
│  THEN                                                                                              │
│      ORDER_QTY = ROUND_UP(REQUIRED_QTY / Order_Multiple) × Order_Multiple                         │
│      ORDER_QTY = GREATEST(ORDER_QTY, Min_Order_Qty)                                               │
│      → Generate Recommendation                                                                     │
│                                                                                                    │
│  ─────────────────────────────────────────────────────────────────────────────────────────────    │
│                                                                                                    │
│  PRIORITY_SCORE = (Stockout_Risk × 0.40)                                                          │
│                 + (Revenue_Impact × 0.30)                                                          │
│                 + (Margin_Impact × 0.20)                                                           │
│                 + (ABC_Class × 0.10)                                                               │
│                                                                                                    │
│  URGENCY_LEVEL:                                                                                    │
│    • CRITICAL: Inventory_Status IN ('STOCKOUT', 'CRITICAL')                                       │
│    • HIGH:     Inventory_Status = 'LOW'                                                            │
│    • MEDIUM:   Days_Of_Supply < 21                                                                 │
│    • LOW:      Otherwise                                                                           │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 PO Recommendation Output Structure

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              PO_RECOMMENDATIONS TABLE STRUCTURE                                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │ RECOMMENDATION #47821                                              Status: PENDING_REVIEW    │ │
│  ├──────────────────────────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                                              │ │
│  │  SKU:          Milestar Patagonia M/T 285/70R17                                             │ │
│  │  DC:           Denver Distribution Center                                                    │ │
│  │  Vendor:       Tireco Asia Pacific (Supplier #V002)                                         │ │
│  │                                                                                              │ │
│  │  ════════════════════════════════════════════════════════════════════════════════════════   │ │
│  │  │ RECOMMENDED ORDER QTY:   700 units                                                  │   │ │
│  │  │ RECOMMENDED ORDER DATE:  March 16, 2026 (TODAY)                                     │   │ │
│  │  │ EXPECTED RECEIPT:        April 3, 2026 (18 days)                                    │   │ │
│  │  │ ESTIMATED COST:          $52,500 ($75/unit)                                         │   │ │
│  │  ════════════════════════════════════════════════════════════════════════════════════════   │ │
│  │                                                                                              │ │
│  │  URGENCY:      🔴 CRITICAL (Score: 87.5/100)                                                │ │
│  │  STOCKOUT IN:  2.6 days at current velocity                                                 │ │
│  │                                                                                              │ │
│  │  ────────────────────────────────────────────────────────────────────────────────────────   │ │
│  │  JUSTIFICATION (AI-Generated):                                                              │ │
│  │                                                                                              │ │
│  │  "Order 700 units of Milestar Patagonia M/T for Denver DC immediately because:              │ │
│  │                                                                                              │ │
│  │   (1) STOCKOUT IMMINENT: Current inventory of 45 units covers only 2.6 days.               │ │
│  │       At forecasted demand of 17 units/day, stockout projected by March 19.                 │ │
│  │                                                                                              │ │
│  │   (2) WEATHER DRIVER: Heavy snowfall forecasted for Denver metro 8-12 inches               │ │
│  │       March 18-20. Historically M/T demand spikes 280% during storms.                       │ │
│  │                                                                                              │ │
│  │   (3) SUPPLY CHAIN DELAY: Port of Long Beach reporting 4-day container delays,             │ │
│  │       extending effective lead time from 14 to 18 days.                                     │ │
│  │                                                                                              │ │
│  │   RISK: Estimated lost revenue of $89,000 from stockout during peak demand."               │ │
│  │  ────────────────────────────────────────────────────────────────────────────────────────   │ │
│  │                                                                                              │ │
│  │  ACTIONS:  [ ✓ APPROVE ]  [ ✎ MODIFY QTY ]  [ ✗ REJECT ]  [ ☐ ADD TO BATCH ]               │ │
│  │                                                                                              │ │
│  └──────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Implementation Phases

### 8.1 Phase Timeline

| Phase | Deliverables | Duration | Dependencies |
|-------|-------------|----------|--------------|
| **1. Foundation** | Database/schemas, Bronze tables, ingestion pipelines | 3-4 weeks | Snowflake account |
| **2. Data Enrichment** | Marketplace datasets, External schema setup | 1-2 weeks | Phase 1 |
| **3. Silver Layer** | Dimension tables (SCD2), Fact tables | 2-3 weeks | Phase 2 |
| **4. Gold Layer** | Dynamic Tables, ML feature engineering | 2-3 weeks | Phase 3 |
| **5. Forecasting** | Cortex ML models, backtesting | 3-4 weeks | Phase 4, historical data |
| **6. PO Engine** | Recommendation stored procedures | 2 weeks | Phase 5 |
| **7. Explainability** | Cortex LLM integration | 1-2 weeks | Phase 6 |
| **8. Orchestration** | Snowflake Tasks DAG, alerting | 1-2 weeks | Phase 7 |
| **9. Dashboard** | Streamlit buyer interface | 2 weeks | Phase 8 |
| **10. ERP Integration** | External functions for PO push | 2-3 weeks | Phase 9, ERP API access |

**Total: 20-28 weeks**

### 8.2 Pilot Strategy

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    RECOMMENDED PILOT APPROACH                                       │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  PILOT SCOPE:                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ • 1 Distribution Center: Denver (weather-sensitive market)                                 │  │
│  │ • Top 50 SKUs by revenue (Pareto 80/20)                                                    │  │
│  │ • 3 key brands: Milestar, Falken, Nitto                                                    │  │
│  │ • 4-week evaluation period                                                                  │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
│  SUCCESS CRITERIA FOR SCALE:                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ • Forecast MAPE < 20% for pilot SKUs                                                       │  │
│  │ • >70% of recommendations accepted by buyers                                               │  │
│  │ • No stockouts for pilot SKUs during period                                                │  │
│  │ • Buyer feedback score > 4/5                                                               │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                    │
│  SCALE ROADMAP:                                                                                    │
│  Week 1-4:   Pilot (Denver, 50 SKUs)                                                              │
│  Week 5-8:   Expand to WEST region (8 DCs, 100 SKUs)                                             │
│  Week 9-12:  Add MIDWEST region (8 DCs, 150 SKUs)                                                │
│  Week 13-16: Full rollout (32 DCs, all SKUs)                                                     │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Success Metrics

### 9.1 Key Performance Indicators

| Metric | Baseline | Target | Measurement |
|--------|----------|--------|-------------|
| **Manual forecasting hours/week** | 40+ hours | <12 hours | Time tracking |
| **Stockout rate (A-class SKUs)** | 8% | <4% | Inventory system |
| **Overstock carrying cost** | $6M/year | $5.1M/year | Finance reports |
| **Forecast accuracy (MAPE)** | N/A | <15% | Model metrics |
| **PO approval rate (auto-generated)** | N/A | >80% | System logs |
| **Days of Supply variance** | ±15 days | ±5 days | Inventory reports |
| **Buyer productivity** | 40% strategic | 80% strategic | Time studies |

### 9.2 Monitoring Dashboard KPIs

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DAILY DASHBOARD SUMMARY                                          │
├────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                    │
│  TODAY'S RECOMMENDATIONS                                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │    TOTAL        │  │   CRITICAL      │  │     HIGH        │  │   MEDIUM/LOW    │              │
│  │      47         │  │      5          │  │      12         │  │      30         │              │
│  │ Recommendations │  │   🔴 Urgent     │  │   🟠 Priority   │  │   🟡🟢 Normal   │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                                                                                                    │
│  TOTAL RECOMMENDED SPEND: $1,247,500          DCs AFFECTED: 18          SKUs AFFECTED: 43        │
│                                                                                                    │
│  ─────────────────────────────────────────────────────────────────────────────────────────────    │
│                                                                                                    │
│  INVENTORY HEALTH                                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │   STOCKOUT      │  │   CRITICAL      │  │     LOW         │  │   HEALTHY       │              │
│  │      12         │  │      28         │  │      145        │  │     3,215       │              │
│  │   SKU-DCs       │  │   SKU-DCs       │  │   SKU-DCs       │  │   SKU-DCs       │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                                                                                                    │
│  ─────────────────────────────────────────────────────────────────────────────────────────────    │
│                                                                                                    │
│  FORECAST PERFORMANCE (7-Day Rolling)                                                              │
│  MAPE: 12.4%     BIAS: +2.1%     FORECAST VALUE ADD: 34% better than naive                       │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Appendix

### 10.1 Solution Validation: Problem-to-Feature Mapping

| Original Problem | Solution Feature | Validation |
|------------------|------------------|------------|
| **Reactive Lag** | Streaming tasks + 14-day weather forecasts + daily demand forecasts | ✅ System anticipates demand before it occurs |
| **Contextual Blindness** | 12+ external Marketplace datasets integrated into ML features | ✅ Weather, port, economic signals inform every recommendation |
| **Operational Friction** | Auto-generated PO recommendations with LLM justifications | ✅ Buyers review/approve, not create from scratch |
| **No Predictive Models** | Cortex ML forecasting at SKU-DC-Day grain | ✅ Statistical forecasts replace guesswork |

### 10.2 Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Cold-start for new SKUs | Use substitute SKU demand as proxy via `RAW_MDM_SUBSTITUTES` |
| Vendor capacity constraints | Check `RAW_SUPP_CAPACITY` before generating recommendations |
| Buyer trust/adoption | Track override rates; prove model value over time |
| Data quality issues | Add `data_quality_score` to forecasts; flag low-confidence predictions |
| ERP integration failures | Retry logic + manual export fallback in Streamlit |

### 10.3 Technology References

- **Snowflake Dynamic Tables**: [Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables)
- **Cortex ML Forecasting**: [Documentation](https://docs.snowflake.com/en/user-guide/ml-powered-functions)
- **Cortex LLM Complete**: [Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- **Snowflake Tasks**: [Documentation](https://docs.snowflake.com/en/user-guide/tasks-intro)
- **Streamlit in Snowflake**: [Documentation](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | March 16, 2026 | Cortex Code | Initial comprehensive architecture document |

---

**END OF DOCUMENT**
