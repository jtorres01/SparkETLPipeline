from DataBase.connection import get_db_connection

def insert_row(row, log_file):

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO orderhistory (
            orderid, orderdate, unitcost, price, orderqty,
            costofsales, sales, profit, channel, promotionname,
            productname, manufacturer, productsubcategory, productcategory,
            region, city, country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (orderid) DO NOTHING;
        """, (
            int(row.OrderID),
            row.OrderDate,
            float(row.UnitCost),
            float(row.Price),
            float(row.OrderQty),
            float(row.CostOfSales),
            float(row.Sales),
            float(row.Profit),
            row.Channel,
            row.PromotionName,
            row.ProductName,
            row.Manufacturer,
            row.ProductSubCategory,
            row.ProductCategory,
            row.Region,
            row.City,
            row.Country
        ))

        if cursor.rowcount == 0:
            log_file.write(f"[DUPLICATE] OrderID {row.OrderID}\n")
            insert_rejected_row(row, log_file)
            conn.commit()
            cursor.close()
            conn.close()
            return "duplicate"

        conn.commit()
        cursor.close()
        conn.close()
        return "inserted"

    except Exception as e:
        cursor.connection.rollback()
        log_file.write(f"[ERROR] OrderID {row.OrderID}: {e}\n")

        conn.commit()
        cursor.close()
        conn.close()
        return "error"


def insert_rejected_row(row, log_file):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO rejecteddata (
            orderid, orderdate, unitcost, price, orderqty,
            costofsales, sales, profit, channel, promotionname,
            productname, manufacturer, productsubcategory, productcategory,
            region, city, country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            int(row.OrderID),
            row.OrderDate,
            float(row.UnitCost),
            float(row.Price),
            float(row.OrderQty),
            float(row.CostOfSales),
            float(row.Sales),
            float(row.Profit),
            row.Channel,
            row.PromotionName,
            row.ProductName,
            row.Manufacturer,
            row.ProductSubCategory,
            row.ProductCategory,
            row.Region,
            row.City,
            row.Country
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        cursor.connection.rollback()
        conn.commit()
        cursor.close()
        conn.close()
        log_file.write(f"[REJECTED TABLE ERROR] {e}\n")
