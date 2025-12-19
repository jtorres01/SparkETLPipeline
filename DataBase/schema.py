from DataBase.connection import get_db_connection

def setup_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("Dropping existing tables if any...")    
    cursor.execute("DROP TABLE orderhistory;")
    cursor.execute("DROP TABLE rejecteddata;")


    print("Creating orderhistory table...")
    cursor.execute("""
            CREATE TABLE orderhistory (
            OrderID INT PRIMARY KEY NOT NULL,
            OrderDate DATE NOT NULL,
            UnitCost DECIMAL(18,8) NOT NULL,
            Price DECIMAL(12,2) NOT NULL,
            OrderQty INT NOT NULL,
            CostOfSales DECIMAL(18,8) NOT NULL,
            Sales DECIMAL(14,2) NOT NULL,
            Profit DECIMAL(18,8) NOT NULL,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150) NOT NULL,
            Manufacturer VARCHAR(150) NOT NULL,
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150) NOT NULL
        );
    """)
    #cursor.execute("""Insert into orderhistory (OrderID, OrderDate, UnitCost, Price, OrderQty, CostOfSales,
    #               Sales, Profit, Channel, PromotionName, ProductName, Manufacturer, ProductSubCategory,
    #                ProductCategory, Region, City, Country) Values
    #                (5, '2024-01-01', 10.00, 12.00, 1, 10.00, 12.00,
    #                2.00, 'Online', 'New Year Promo', 'Gadget Pro',
    #                'TechCorp', 'Gadgets', 'Electronics', 'North America',
    #                'New York', 'USA');
    #""")

    print("Creating rejecteddata table...")
    cursor.execute("""
        CREATE TABLE rejecteddata (
            OrderID INT,
            OrderDate DATE,
            UnitCost DECIMAL(18,8),
            Price DECIMAL(12,2),
            OrderQty INT,
            CostOfSales DECIMAL(18,8),
            Sales DECIMAL(14,2),
            Profit DECIMAL(18,8) ,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150),
            Manufacturer VARCHAR(150),
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

