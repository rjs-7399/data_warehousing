/*------------------------------------------------------------------------------
    ORDER ITEM FACT TABLE CREATION AND POPULATION

    This script creates and populates the order_item_fact table, which is the
    central fact table in the food delivery data warehouse. It contains detailed
    information about each food order item, linking to all relevant dimensions
    and storing key measures for analysis.

    The script also establishes referential integrity through foreign key
    constraints to ensure data quality and consistency.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
                                    System Steps
------------------------------------------------------------------------------*/
-- Create a fact table for order items in the consumption layer
-- Create a merge statement to populate the fact table from source tables
-- Add foreign key constraints to ensure referential integrity

/*------------------------------------------------------------------------------
    PART 1: INITIAL SETUP AND CONTEXT
------------------------------------------------------------------------------*/

-- Set up the session context
use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema stage_sch;

/*------------------------------------------------------------------------------
    PART 2: ORDER ITEM FACT TABLE CREATION
------------------------------------------------------------------------------*/

-- Create the Order Item fact table
-- This is the central fact table connecting all dimensions in the star schema
create or replace table consumption_sch.order_item_fact (
    order_item_fact_sk NUMBER AUTOINCREMENT comment 'Surrogate Key (EDW)',          -- Surrogate key for the fact table
    order_item_id NUMBER  comment 'Order Item FK (Source System)',                  -- Natural key from the source data
    order_id NUMBER  comment 'Order FK (Source System)',                            -- Reference to the order dimension
    customer_dim_key NUMBER  comment 'Order FK (Source System)',                    -- Reference to the customer dimension
    customer_address_dim_key NUMBER,                                                -- Reference to the customer address dimension
    restaurant_dim_key NUMBER,                                                      -- Reference to the restaurant dimension
    restaurant_location_dim_key NUMBER,                                             -- Reference to the restaurant location dimension
    menu_dim_key NUMBER,                                                            -- Reference to the menu dimension
    delivery_agent_dim_key NUMBER,                                                  -- Reference to the delivery agent dimension
    order_date_dim_key NUMBER,                                                      -- Reference to the date dimension
    quantity NUMBER,                                                                -- Measure: Number of items ordered
    price NUMBER(10, 2),                                                            -- Measure: Unit price of the item
    subtotal NUMBER(10, 2),                                                         -- Measure: Total price for the line item
    delivery_status VARCHAR,                                                        -- Status of the delivery (e.g., Delivered, Cancelled)
    estimated_time VARCHAR                                                          -- Estimated delivery time
)
comment = 'The item order fact table that has item level price, quantity and other details.';

/*------------------------------------------------------------------------------
    PART 3: FACT TABLE POPULATION
------------------------------------------------------------------------------*/

-- Populate the fact table using a MERGE statement
-- This joins all relevant tables from the clean layer and links to dimensional keys
MERGE INTO
    consumption_sch.order_item_fact AS target
USING (
    SELECT
        oi.Order_Item_ID AS order_item_id,                          -- Primary key from order items
        oi.Order_ID_fk AS order_id,                                 -- Foreign key to orders
        c.CUSTOMER_HK AS customer_dim_key,                          -- Link to customer dimension
        ca.CUSTOMER_ADDRESS_HK AS customer_address_dim_key,         -- Link to customer address dimension
        r.RESTAURANT_HK AS restaurant_dim_key,                      -- Link to restaurant dimension
        rl.restaurant_location_hk as restaurant_location_dim_key,   -- Link to restaurant location dimension
        m.Menu_Dim_HK AS menu_dim_key,                              -- Link to menu dimension
        da.DELIVERY_AGENT_HK AS delivery_agent_dim_key,             -- Link to delivery agent dimension
        dd.DATE_DIM_HK AS order_date_dim_key,                       -- Link to date dimension
        oi.Quantity::number(2) AS quantity,                         -- Convert quantity to number type
        oi.Price AS price,                                          -- Price per unit
        oi.Subtotal AS subtotal,                                    -- Total price for line item
        o.PAYMENT_METHOD,                                           -- Payment method used
        d.delivery_status AS delivery_status,                       -- Current delivery status
        d.estimated_time AS estimated_time                          -- Estimated delivery time
    FROM
        clean_sch.order_item_stm oi                                 -- Start with order items
    JOIN
        clean_sch.orders_stm o ON oi.Order_ID_fk = o.Order_ID       -- Join with orders
    JOIN
        clean_sch.delivery_stm d ON o.Order_ID = d.Order_ID_fk      -- Join with delivery info
    JOIN
        consumption_sch.CUSTOMER_DIM c ON o.Customer_ID_fk = c.customer_id   -- Join with customer dimension
    JOIN
        consumption_sch.CUSTOMERADDRESS_DIM ca ON c.Customer_ID = ca.CUSTOMER_ID_fk   -- Join with customer address dimension
    JOIN
        consumption_sch.restaurant_dim r ON o.Restaurant_ID_fk = r.restaurant_id   -- Join with restaurant dimension
    JOIN
        consumption_sch.menu_dim m ON oi.MENU_ID_fk = m.menu_id     -- Join with menu dimension
    JOIN
        consumption_sch.deliveryagent_dim da ON d.Delivery_Agent_ID_fk = da.delivery_agent_id   -- Join with delivery agent dimension
    JOIN
        consumption_sch.restaurant_location_dim rl ON r.LOCATION_ID_FK = rl.location_id   -- Join with restaurant location dimension
    JOIN
        CONSUMPTION_SCH.DATE_DIM dd ON dd.calendar_date = date(o.order_date)   -- Join with date dimension
) AS source_stm
ON
    target.order_item_id = source_stm.order_item_id AND             -- Match on natural key
    target.order_id = source_stm.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.customer_dim_key = source_stm.customer_dim_key,
        target.customer_address_dim_key = source_stm.customer_address_dim_key,
        target.restaurant_dim_key = source_stm.restaurant_dim_key,
        target.restaurant_location_dim_key = source_stm.restaurant_location_dim_key,
        target.menu_dim_key = source_stm.menu_dim_key,
        target.delivery_agent_dim_key = source_stm.delivery_agent_dim_key,
        target.order_date_dim_key = source_stm.order_date_dim_key,
        target.quantity = source_stm.quantity,
        target.price = source_stm.price,
        target.subtotal = source_stm.subtotal,
        target.delivery_status = source_stm.delivery_status,
        target.estimated_time = source_stm.estimated_time
WHEN NOT MATCHED THEN
    INSERT (
        order_item_id,
        order_id,
        customer_dim_key,
        customer_address_dim_key,
        restaurant_dim_key,
        restaurant_location_dim_key,
        menu_dim_key,
        delivery_agent_dim_key,
        order_date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    )
    VALUES (
        source_stm.order_item_id,
        source_stm.order_id,
        source_stm.customer_dim_key,
        source_stm.customer_address_dim_key,
        source_stm.restaurant_dim_key,
        source_stm.restaurant_location_dim_key,
        source_stm.menu_dim_key,
        source_stm.delivery_agent_dim_key,
        source_stm.order_date_dim_key,
        source_stm.quantity,
        source_stm.price,
        source_stm.subtotal,
        source_stm.delivery_status,
        source_stm.estimated_time
    );

/*------------------------------------------------------------------------------
    PART 4: REFERENTIAL INTEGRITY CONSTRAINTS
------------------------------------------------------------------------------*/

-- Add foreign key constraints to ensure referential integrity
-- These constraints enforce the star schema relationships

-- Link to Customer Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_customer_dim
    foreign key (customer_dim_key)
    references consumption_sch.customer_dim (customer_hk);

-- Link to Customer Address Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_customer_address_dim
    foreign key (customer_address_dim_key)
    references consumption_sch.customeraddress_dim (CUSTOMER_ADDRESS_HK);

-- Link to Restaurant Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_restaurant_dim
    foreign key (restaurant_dim_key)
    references consumption_sch.restaurant_dim (restaurant_hk);

-- Link to Restaurant Location Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_restaurant_location_dim
    foreign key (restaurant_location_dim_key)
    references consumption_sch.restaurant_location_dim (restaurant_location_hk);

-- Link to Menu Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_menu_dim
    foreign key (menu_dim_key)
    references consumption_sch.menu_dim (menu_dim_hk);

-- Link to Delivery Agent Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_delivery_agent_dim
    foreign key (delivery_agent_dim_key)
    references consumption_sch.deliveryagent_dim (delivery_agent_hk);

-- Link to Date Dimension
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_delivery_date_dim
    foreign key (order_date_dim_key)
    references consumption_sch.date_dim (date_dim_hk);

/*------------------------------------------------------------------------------
    USAGE NOTES

    1. This fact table is the central component of the star schema, connecting
       all dimension tables and storing key measures.

    2. The fact table contains both order-level and item-level information,
       allowing for analysis at different levels of granularity.

    3. Foreign key constraints ensure data integrity and prevent orphaned
       records in the fact table.

    4. The merge statement includes business logic for handling both new
       records (inserts) and updates to existing records.

    5. The granularity of this fact table is at the order item level, which
       means each row represents a single item within an order.

    6. Key measures include quantity, price, and subtotal, enabling various
       revenue and order volume analyses.

    7. The foreign keys to dimension tables enable slicing and dicing of
       the data across multiple business dimensions like customer, restaurant,
       location, menu item, and time.
------------------------------------------------------------------------------*/