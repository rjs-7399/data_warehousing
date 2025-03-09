# Swiggy Data Warehouse Implementation


This project implements a comprehensive data warehouse solution for Swiggy's food delivery platform using Snowflake. The implementation follows a multi-layered architecture designed to transform operational data into an analytics-ready format that supports business intelligence, reporting, and data science initiatives.
The data warehouse integrates data from multiple source systems into a cohesive dimensional model, enabling cross-functional analysis of customer behavior, restaurant performance, delivery operations, and revenue metrics. By implementing slowly changing dimensions (SCD Type 2) for key entities, the system preserves historical accuracy while maintaining current views of the business.
The solution includes:

- A three-tiered data architecture (Stage, Clean, Consumption layers)
- Automated data transformation processes
- Dimensional modeling with fact and dimension tables
- Pre-built KPI views for common business metrics
- An interactive Streamlit dashboard for business users
- The fundamental purpose behind creating this project is to showcase to our business team how we can design a modern data warehouse with effective data modeling. For this implementation, I've chosen Snowflake as the primary platform.

## Why Snowflake?

- For this implementation, Snowflake was selected over alternatives like AWS Redshift or GCP BigQuery for several key reasons:

- **Separation of storage and compute**: Allowing for independent scaling of resources
- **Zero-management infrastructure**: Reducing operational overhead
- **Pay-per-use model**: Optimizing cost efficiency
- **Seamless scalability**: Supporting business growth without infrastructure constraints
- **Multi-cluster architecture**: Enabling concurrent workloads without performance degradation
- **Native support for semi-structured data**: Simplifying the integration of various data formats
## Table of Contents

- [System Architecture](#system-architecture)
- [Data Warehouse and Data Modeling](#data-warehouse-and-data-modeling)
- [OLTP vs OLAP](#oltp-vs-olap)
- [Entities](#entities)
  - [Restaurant](#restaurant)
  - [Location](#location)
  - [Menu](#menu)
  - [Customer](#customer)
  - [Address](#address)
  - [Order](#order)
  - [Order Item](#order-item)
  - [Delivery Agent](#delivery-agent)
  - [Delivery](#delivery)
  - [Login Audit](#login-audit)
- [KPI Views](#kpi-views)
- [Streamlit Dashboard](#streamlit-dashboard)

## System Architecture

This implementation follows a standard batch processing pattern where data flows from source systems through a multi-layered architecture in Snowflake:

![Snowflake Data Warehouse Architecture](project_snapshots/snowflake_dw_architecture.jpg)

1. **Stage Layer**: Raw data landing from source systems
2. **Clean Layer**: Data with appropriate types and business rules applied
3. **Consumption Layer**: Dimensional model with SCD Type 2 implementation for historical tracking

For this project, files were manually loaded into Snowflake. However, in a production-grade implementation, an Apache Airflow pipeline would orchestrate the entire process:

![Airflow Snowflake Pipeline](project_snapshots/airflow_pipeline_architecture.jpg)

The production data pipeline would consist of these key stages:
1. **Data Extraction**: Loading data from source systems to Snowflake Stage locations
2. **Stage to Clean Transformation**: Applying data type conversions and validations
3. **Clean to Consumption Transformation**: Implementing dimensional modeling with SCD Type 2
4. **KPI Generation**: Creating analytical views for business consumption
5. **Monitoring & Alerting**: Ensuring data quality and pipeline health

Each entity would have its own DAG (Directed Acyclic Graph) in Airflow, with appropriate dependencies to ensure data consistency.

## Data Warehouse and Data Modeling

A data warehouse is a centralized repository designed for analytical processing and business intelligence. For this Swiggy implementation, we've employed a star schema dimensional model in the consumption layer:

- **Fact Tables**: Order Item Fact (central table containing measurements and metrics)
- **Dimension Tables**: Restaurant, Location, Menu, Customer, Address, Delivery Agent, Date

This model optimizes for query performance, business understandability, and analytical flexibility. We've implemented SCD Type 2 for dimensions to track historical changes, which is essential for accurate point-in-time reporting.

## Source System ER Diagram

```mermaid
erDiagram
    location {
        serial4 locationid PK
        varchar100 city
        varchar100 state
        varchar10 zipcode
        varchar10 activeflag
        timestamptz createdate
        timestamptz modifieddate
    }

    customeraddressbook {
        serial4 addressid PK
        int4 customerid FK
        varchar10 flatno
        varchar10 houseno
        varchar10 floor
        varchar100 building
        varchar100 landmark
        text coordinates
        varchar10 primaryflag
        varchar20 addresstype
        timestamptz createdate
        timestamptz modifieddate
        varchar50 locality
        varchar50 city
        varchar50 state
        int4 pincode
    }

    customer {
        serial4 customerid PK
        varchar100 name
        varchar10 mobile
        varchar100 email
        varchar20 loginbyusing
        varchar10 gender
        date dob
        date anniversary
        jsonb preferences
        timestamptz createdate
        timestamptz modifieddate
    }

    loginaudit {
        serial4 loginid PK
        int4 customerid FK
        varchar20 logintype
        varchar20 deviceinterface
        varchar50 mobiledevicename
        varchar50 webinterface
        timestamptz lastlogin
    }

    deliveryagent {
        serial4 deliveryagentid PK
        varchar100 name
        varchar15 phone
        varchar50 vehicletype
        int4 locationid FK
        varchar10 status
        numeric21 rating
        timestamptz createdate
        timestamptz modifieddate
        varchar50 gender
    }

    delivery {
        serial4 deliveryid PK
        int4 orderid FK
        int4 deliveryagentid FK
        varchar15 deliverystatus
        interval estimatedtime
        timestamptz deliverydate
        timestamptz createdate
        timestamptz modifieddate
        int4 deliveryaddress
    }

    orders {
        serial4 orderid PK
        int4 customerid FK
        int4 restaurantid FK
        timestamptz orderdate
        numeric102 totalamount
        varchar15 status
        varchar15 paymentmethod
        timestamptz createdate
        timestamptz modifieddate
    }

    restaurant {
        serial4 restaurantid PK
        varchar100 name
        text cuisinetype
        numeric102 pricing_for_2
        varchar15 restaurant_phone
        varchar100 operatinghours
    }

    menu {
        serial4 menuid PK
        int4 restaurantid FK
        varchar100 itemname
        text description
    }

    orderitem {
        serial4 orderitemid PK
        int4 orderid FK
        int4 menuid FK
        int4 quantity
        numeric102 price
        numeric102 subtotal
    }

    location ||--o{ deliveryagent : "locationid"
    location ||--o{ restaurant : "locationid"
    customer ||--o{ customeraddressbook : "customerid"
    customer ||--o{ loginaudit : "customerid"
    customer ||--o{ orders : "customerid"
    restaurant ||--o{ menu : "restaurantid"
    restaurant ||--o{ orders : "restaurantid"
    orders ||--|| delivery : "orderid"
    orders ||--o{ orderitem : "orderid"
    menu ||--o{ orderitem : "menuid"
    deliveryagent ||--o{ delivery : "deliveryagentid"
```

## Data Warehouse Model

```mermaid
erDiagram
    ORDER_ITEM_FACT {
        bigint ORDER_ITEM_FACT_SK PK
        bigint ORDER_ITEM_ID
        bigint ORDER_ID
        bigint CUSTOMER_DIM_KEY FK
        bigint CUSTOMER_ADDRESS_DIM_KEY FK
        bigint RESTAURANT_DIM_KEY FK
        bigint RESTAURANT_LOCATION_DIM_KEY FK
        bigint MENU_DIM_KEY FK
        bigint DELIVERY_AGENT_DIM_KEY FK
        bigint ORDER_DATE_DIM_KEY FK
        int QUANTITY
        decimal PRICE
        decimal SUBTOTAL
        varchar DELIVERY_STATUS
        timestamp ESTIMATED_TIME
    }

    CUSTOMER_DIM {
        bigint CUSTOMER_HK PK
        varchar CUSTOMER_ID
        varchar NAME
        varchar MOBILE
        varchar EMAIL
        varchar LOGIN_BY_USING
        varchar GENDER
        date DOB
        date ANNIVERSARY
        jsonb PREFERENCES
        date EFF_START_DATE
        date EFF_END_DATE
        boolean IS_CURRENT
    }

    CUSTOMER_ADDRESS_DIM {
        bigint CUSTOMER_ADDRESS_HK PK
        varchar ADDRESS_ID
        varchar CUSTOMER_ID_FK
        varchar FLAT_NO
        varchar HOUSE_NO
        varchar FLOOR
        varchar BUILDING
        varchar LANDMARK
        varchar LOCALITY
        varchar CITY
        varchar STATE
        varchar PINCODE
        point COORDINATES
        boolean PRIMARY_FLAG
        varchar ADDRESS_TYPE
        date EFF_START_DATE
        date EFF_END_DATE
        boolean IS_CURRENT
    }

    DATE_DIM {
        bigint DATE_DIM_HK PK
        date CALENDAR_DATE
        int YEAR
        int QUARTER
        int MONTH
        int WEEK
        int DAY_OF_YEAR
        int DAY_OF_WEEK
        int DAY_OF_THE_MOTH
        varchar DAY_NAME
    }

    DELIVERY_AGENT_DIM {
        bigint DELIVERY_AGENT_HK PK
        varchar DELIVERY_AGENT_ID
        varchar NAME
        varchar PHONE
        varchar VEHICLE_TYPE
        varchar LOCATION_ID_FK
        varchar STATUS
        varchar GENDER
        decimal RATING
        date EFF_START_DATE
        date EFF_END_DATE
        boolean IS_CURRENT
    }

    RESTAURANT_LOCATION_DIM {
        bigint RESTAURANT_LOCATION_HK PK
        varchar LOCATION_ID
        varchar CITY
        varchar STATE
        varchar STATE_CODE
        boolean IS_UNION_TERRITORY
        boolean CAPITAL_CITY_FLAG
        varchar CITY_TIER
        varchar ZIP_CODE
        boolean ACTIVE_FLAG
        date EFF_START_DATE
        date EFF_END_DATE
        boolean CURRENT_FLAG
    }

    RESTAURANT_DIM {
        bigint RESTAURANT_HK PK
        varchar RESTAURANT_ID
        varchar NAME
        varchar CUSINE_TYPE
        decimal PRICING_FOR_TWO
        varchar RESTAURANT_PHONE
        varchar OPERATING_HOURS
        varchar LOCATION_ID_FK
        boolean ACTIVE_FLAG
        varchar OPEN_STATUS
        varchar LOCALITY
        text RESTAURANT_ADDRESS
    }

    MENU_DIM {
        bigint MENU_DIM_HK PK
        varchar MENU_ID
        varchar RESTAURANT_ID_FK
        varchar ITEM_NAME
        text DESCRIPTION
        decimal PRICE
        varchar CATEGORY
        boolean AVAILABILITY
        varchar ITEM_TYPE
        date EFF_START_DATE
        date EFF_END_DATE
        boolean IS_CURRENT
    }

    CUSTOMER_DIM ||--o{ ORDER_ITEM_FACT : references
    CUSTOMER_ADDRESS_DIM ||--o{ ORDER_ITEM_FACT : references
    DATE_DIM ||--o{ ORDER_ITEM_FACT : references
    DELIVERY_AGENT_DIM ||--o{ ORDER_ITEM_FACT : references
    RESTAURANT_LOCATION_DIM ||--o{ ORDER_ITEM_FACT : references
    RESTAURANT_DIM ||--o{ ORDER_ITEM_FACT : references
    MENU_DIM ||--o{ ORDER_ITEM_FACT : references
```

## OLTP vs OLAP

Swiggy's operational database (OLTP) and this analytical data warehouse (OLAP) serve fundamentally different purposes:

| OLTP (Operational DB)                | OLAP (Data Warehouse)                 |
|--------------------------------------|---------------------------------------|
| Handles day-to-day transactions      | Supports analytical processing        |
| Optimized for fast inserts/updates   | Optimized for complex queries         |
| Normalized structure                 | Denormalized star schema              |
| Current data only                    | Historical data with time dimension   |
| Transaction-focused                  | Analysis-focused                      |

With millions of users registering and performing transactions daily on Swiggy, the OLTP system manages the core business operations. Meanwhile, this data warehouse stores transformed, integrated data optimized for business analysis and machine learning model development. This separation ensures operational systems maintain peak performance while still enabling deep analytical capabilities.

## Entities

### Restaurant

![Restaurant Entity](project_snapshots/restaurant_entity.jpg)

- The Restaurant entity contains information about food establishments on the Swiggy platform. 
- It includes details like name, cuisine type, pricing, and operating hours. 
- Restaurants are connected to Locations (many-to-one) and have a one-to-many relationship with Menu items. 
- They also receive multiple Orders from customers.

### Location

![Location Entity](project_snapshots/location_entity.jpg)

- The Location entity stores geographical information that serves both restaurants and delivery agents. 
- It includes city, state, zip code, and other regional attributes. 
- Each location can have multiple restaurants and delivery agents assigned to it, forming the foundation for geographical analysis.

### Menu

![Menu Entity](project_snapshots/menu_entity.jpg)

- The Menu entity represents food items available at restaurants. 
- Each restaurant has multiple menu items, and these items can appear in multiple order items. 
- Menu data includes item name, description, price, category, and availability status.

### Customer

![Customer Entity](project_snapshots/customer_entity.jpg)

- The Customer entity stores information about Swiggy users. 
- This includes personal details like name, contact information, preferences, and demographic data. 
- Each customer can have multiple addresses, place multiple orders, and generate multiple login records.

### Address

![Address Entity](project_snapshots/customer_address_entity.jpg)

- The Customer Address Book entity contains delivery locations for customers. 
- A single customer can have multiple addresses stored (one-to-many relationship), enabling delivery to different locations. 
- Each address includes detailed location information and is referenced in delivery records.

### Order

![Order Entity](project_snapshots/orders_entity.jpg)

- The Orders entity tracks food purchases made by customers. 
- Each order belongs to one customer and one restaurant, with a one-to-many relationship to order items. 
- Orders also have a one-to-one relationship with delivery records, connecting the purchase to its fulfillment process.

### Order Item

![Order Item Entity](project_snapshots/order_item_entity.jpg)

- The Order Item entity represents individual food items within an order. 
- It forms the bridge between the Orders and Menu entities, recording what items were ordered, their quantity, and pricing. 
- This granular data enables detailed analysis of item popularity and revenue generation.

### Delivery Agent

![Delivery Agent Entity](project_snapshots/delivery_agent_entity.jpg)

- The Delivery Agent entity contains information about the personnel who deliver orders. 
- Each agent is assigned to a specific location and can handle multiple deliveries. 
- The entity includes personal details, vehicle information, status, and performance metrics like ratings.

### Delivery

![Delivery Entity](project_snapshots/delivery_entity.jpg)

- The Delivery entity tracks the fulfillment of orders. 
- It has a one-to-one relationship with orders and is associated with one delivery agent. 
- This entity records delivery status, timing information, and the destination address, enabling analysis of delivery performance.

### Login Audit

- The Login Audit entity tracks customer login activity across the platform. 
- Connected to the Customer entity in a one-to-many relationship, it records login type, device information, and timestamps, supporting security monitoring and user behavior analysis.

### Order Item Fact

![Order Item Fact](project_snapshots/order_item_fact.jpg)

- The Delivery Agent entity contains information about the personnel who deliver orders. 
- Each agent is assigned to a specific location and can handle multiple deliveries. 
- The entity includes personal details, vehicle information, status, and performance metrics like ratings.

## KPI Views

To support business analysis, we've created several analytical views built on the dimensional model:

1. **Yearly Revenue KPIs** (`vw_yearly_revenue_kpis`): 
   - Aggregates revenue metrics by year
   - Includes total revenue, order count, average revenue per order/item, and maximum order value
   - Supports year-over-year analysis and strategic planning

2. **Monthly Revenue KPIs** (`vw_monthly_revenue_kpis`): 
   - Breaks down revenue performance by month
   - Tracks seasonal patterns and month-to-month trends
   - Essential for identifying growth patterns and cyclical behaviors

3. **Daily Revenue KPIs** (`vw_daily_revenue_kpis`): 
   - Provides day-by-day performance metrics
   - Identifies specific high-performance or problematic days
   - Supports detailed operational analysis

4. **Day-of-Week Revenue KPIs** (`vw_day_revenue_kpis`): 
   - Analyzes performance patterns by day of week
   - Highlights weekday vs. weekend trends
   - Informs promotional and staffing decisions

5. **Monthly Revenue by Restaurant** (`vw_monthly_revenue_by_restaurant`): 
   - Segments performance by restaurant
   - Tracks individual restaurant metrics over time
   - Enables competitive analysis and restaurant-specific strategies

These views filter for delivered orders to focus on completed sales, ensuring business metrics reflect actual revenue generated rather than pending or canceled orders.

## Streamlit Dashboard


The Streamlit dashboard provides a visual interface for business users to interact with the data warehouse. The dashboard includes:

- Revenue metrics displayed at different time granularities (yearly, monthly, daily)
- Year-over-year comparisons with delta indicators
- Interactive visualizations of revenue trends
- Top-performing restaurant rankings
- Filterable views by time period

This dashboard enables business stakeholders to gain insights without requiring SQL knowledge, democratizing data access across the organization.

![Revenue Dashboard](project_snapshots/yearly_revenue.jpg)
![Revenue Dashboard](project_snapshots/monthly_revenue.jpg)
![Revenue Dashboard](project_snapshots/top_ten_restaurants.jpg)
