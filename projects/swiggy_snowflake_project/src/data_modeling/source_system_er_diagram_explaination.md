# Swiggy Database Entity Relationships

## 1. Customer-Related Relationships

### Customer → Customer Address Book (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via CustomerID (Foreign Key in CustomerAddressBook)
- **Details**:
  - A single customer can have multiple addresses stored
  - Each address must belong to exactly one customer
  - Enables multiple delivery locations per customer

### Customer → Login Audit (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via CustomerID (Foreign Key in LoginAudit)
- **Details**:
  - A customer can have multiple login records
  - Each login record belongs to one customer
  - Tracks customer's login history across different devices

### Customer → Orders (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via CustomerID (Foreign Key in Orders)
- **Details**:
  - A customer can place multiple orders
  - Each order belongs to one customer
  - Maintains order history per customer

## 2. Restaurant-Related Relationships

### Restaurant → Menu (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via RestaurantID (Foreign Key in Menu)
- **Details**:
  - A restaurant can have multiple menu items
  - Each menu item belongs to one restaurant
  - Allows restaurants to maintain their food catalog

### Restaurant → Orders (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via RestaurantID (Foreign Key in Orders)
- **Details**:
  - A restaurant can receive multiple orders
  - Each order is associated with one restaurant
  - Tracks all orders received by restaurants

### Restaurant → Location (Many:1)
- **Relationship Type**: Many-to-One
- **Connection**: Via LocationID (Foreign Key in Restaurant)
- **Details**:
  - Each restaurant belongs to one location
  - A location can have multiple restaurants
  - Helps in geographical organization of restaurants

## 3. Order-Related Relationships

### Orders → Order Items (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via OrderID (Foreign Key in OrderItem)
- **Details**:
  - One order can contain multiple order items
  - Each order item belongs to one order
  - Represents individual items within an order

### Orders → Delivery (1:1)
- **Relationship Type**: One-to-One
- **Connection**: Via OrderID (Foreign Key in Delivery)
- **Details**:
  - Each order has exactly one delivery record
  - Each delivery record belongs to one order
  - Manages delivery information for each order

## 4. Delivery-Related Relationships

### Delivery Agent → Delivery (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via DeliveryAgentID (Foreign Key in Delivery)
- **Details**:
  - A delivery agent can handle multiple deliveries
  - Each delivery is assigned to one delivery agent
  - Tracks delivery agent assignments

### Delivery Agent → Location (Many:1)
- **Relationship Type**: Many-to-One
- **Connection**: Via LocationID (Foreign Key in DeliveryAgent)
- **Details**:
  - Each delivery agent is assigned to one location
  - A location can have multiple delivery agents
  - Manages delivery agent distribution across locations

## 5. Menu-Related Relationships

### Menu → Order Items (1:Many)
- **Relationship Type**: One-to-Many
- **Connection**: Via MenuID (Foreign Key in OrderItem)
- **Details**:
  - A menu item can appear in multiple order items
  - Each order item refers to one menu item
  - Links orders to specific menu items

## Database Design Benefits

### Normalization
- Primary keys uniquely identify each record
- Foreign keys maintain referential integrity
- Relationships are properly structured to avoid data redundancy

### Functionality
- Efficient tracking of customer orders and delivery status
- Streamlined management of restaurant menus and locations
- Organized handling of delivery agent assignments
- Comprehensive maintenance of customer data
- Systematic processing of order items

### Scalability
- Design supports multiple addresses per customer
- Handles multiple orders across restaurants
- Manages multiple delivery agents per location
- Tracks detailed order history
- Supports extensive menu items per restaurant