### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
from collections import defaultdict
import datetime

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

mydict=defaultdict(list)
def read_data_csv():
    global mydict
    header=None
    data_all=[]
    with open("data.csv") as lines:
        for line in lines:
            if not header:
                header=line.strip().split("\t")
                continue
            data=line.strip().split('\t')
            data_all.append(data)
    mydict=dict(zip(header,list(zip(*data_all))))
read_data_csv()
def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Region ( 
            RegionID Integer not null primary key,
            Region Text not null);'''
        insert_sql='''INSERT INTO Region(Region) VALUES(?)'''
        create_table(conn,create_sql,'Region')
        region_data=sorted(list(zip(set(mydict["Region"]))))
        cur.executemany(insert_sql,region_data)
        conn.commit()
    conn.close()
    pass
    ### END SOLUTION
# step1_create_region_table('data.csv',"normalized.db")
def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_region_sql="SELECT RegionID,Region FROM Region"
        region_val=execute_sql_statement(get_region_sql,conn)
        region_fk_lookup = {}
        for row in region_val:
            key, text = row
            region_fk_lookup[text] = key
    conn.close()
    return region_fk_lookup
    pass

    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Country(
        [CountryID] integer not null Primary key,
        [Country] Text not null,
        [RegionID] integer not null,
        foreign key(RegionID) references Region(RegionID) ON DELETE CASCADE);'''
        insert_sql='''INSERT INTO Country(Country,RegionID) VALUES(?,?)'''
        create_table(conn,create_sql,'Country')
        fk_lookup=step2_create_region_to_regionid_dictionary(normalized_database_filename)
        mydata_data=sorted(set(list(zip(mydict["Country"],(fk_lookup[ele] for ele in mydict['Region'])))))
        cur.executemany(insert_sql,mydata_data)
        conn.commit()
    conn.close()
    pass
         
    ### END SOLUTION
# step3_create_country_table('data.csv',"normalized.db")

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_country_sql="SELECT CountryID,Country FROM Country"
        country_val=execute_sql_statement(get_country_sql,conn)
        country_fk_lookup = {}
        for row in country_val:
            key, text = row
            country_fk_lookup[text] = key
    conn.close()
    return country_fk_lookup
    pass

    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Customer( 
        [CustomerID] integer not null Primary Key, 
        [FirstName] Text not null, 
        [LastName] Text not null, 
        [Address] Text not null, 
        [City] Text not null, 
        [CountryID] integer not null,
        foreign key(CountryID) REFERENCES Country(CountryID) ON DELETE CASCADE); '''
        insert_sql='''INSERT INTO Customer(FirstName,LastName,Address,City,CountryID) VALUES(?,?,?,?,?)'''
        create_table(conn,create_sql,'Customer')
        fk_lookup=step4_create_country_to_countryid_dictionary(normalized_database_filename)
        mydata_data=(set(list(zip([ele.split(' ',1)[0].strip() for ele in mydict["Name"]],[ele.split(' ',1)[1].strip() for ele in mydict["Name"]],mydict["Address"],mydict["City"],(fk_lookup[ele] for ele in mydict['Country'])))))
        mydata_data=sorted(mydata_data,key=lambda ele: ele[0]+ele[1])
        cur.executemany(insert_sql,mydata_data)
        conn.commit()
    conn.close()
    pass

    ### END SOLUTION
# step5_create_customer_table('data.csv',"normalized.db")

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_customer_sql="SELECT CustomerID,FirstName || ' ' || LastName FROM Customer"
        customer_val=execute_sql_statement(get_customer_sql,conn)
        customer_fk_lookup = {}
        for row in customer_val:
            key, text = row
            customer_fk_lookup[text] = key
    conn.close()
    return customer_fk_lookup
    pass

    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE ProductCategory(
            [ProductCategoryID] integer not null Primary Key, 
            [ProductCategory] Text not null, 
            [ProductCategoryDescription] Text not null); '''
        insert_sql='''INSERT INTO ProductCategory(ProductCategory,ProductCategoryDescription) VALUES(?,?)'''
        create_table(conn,create_sql,'ProductCategory')
        mydata_data=sorted(set(list(zip(mydict['ProductCategory'][0].split(';'),mydict['ProductCategoryDescription'][0].split(';')))))
        cur.executemany(insert_sql,mydata_data)
        conn.commit()
    conn.close()
    pass
   
    ### END SOLUTION
# step7_create_productcategory_table('data.csv',"normalized.db")
def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_category_sql="SELECT ProductCategoryID,ProductCategory FROM ProductCategory"
        category_val=execute_sql_statement(get_category_sql,conn)
        category_fk_lookup = {}
        for row in category_val:
            key, text = row
            category_fk_lookup[text] = key
    conn.close()
    return category_fk_lookup
    pass

    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Product(
            [ProductID] integer not null Primary key,
            [ProductName] Text not null,
            [ProductUnitPrice] Real not null,
            [ProductCategoryID] integer not null,
            foreign key(ProductCategoryID) references ProductCategory(ProductCategoryID) ON DELETE CASCADE); '''
        insert_sql='''INSERT INTO Product(ProductName,ProductUnitPrice,ProductCategoryID) VALUES(?,?,?)'''
        create_table(conn,create_sql,'Product')
        fk_lookup=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
        mydata_data=sorted(set(list(zip(mydict['ProductName'][0].split(';'),mydict['ProductUnitPrice'][0].split(';'),(fk_lookup[ele] for ele in mydict['ProductCategory'][0].split(';'))))))
        cur.executemany(insert_sql,mydata_data)
        conn.commit()
    conn.close()
    pass
   
    ### END SOLUTION
# step9_create_product_table('data.csv',"normalized.db")

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_product_sql="SELECT ProductID,ProductName FROM Product"
        product_val=execute_sql_statement(get_product_sql,conn)
        product_fk_lookup = {}
        for row in product_val:
            key, text = row
            product_fk_lookup[text] = key
    conn.close()
    return product_fk_lookup
    pass

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE OrderDetail(
            [OrderID] integer not null Primary Key,
            [CustomerID] integer not null,
            [ProductID] integer not null,
            [OrderDate] integer not null,
            [QuantityOrdered] integer not null,
            foreign key(CustomerID) references Customer(CustomerID) ON DELETE CASCADE,
            foreign key(ProductID) references Product(ProductID) ON DELETE CASCADE); '''
        insert_sql='''INSERT INTO OrderDetail(CustomerID,ProductID,QuantityOrdered,OrderDate) VALUES(?,?,?,?)'''
        create_table(conn,create_sql,'OrderDetail')
        prod_fk_lookup=step10_create_product_to_productid_dictionary(normalized_database_filename)
        cust_fk_lookup=step6_create_customer_to_customerid_dictionary(normalized_database_filename)
        for ele in range(len(mydict['Name'])):
            mydata_data=list(zip((cust_fk_lookup[ele] for ele in [mydict['Name'][ele]]*len(mydict['ProductName'][ele].split(';'))),(prod_fk_lookup[ele] for ele in mydict['ProductName'][ele].split(';')),list(map(int,mydict['QuantityOrderded'][ele].split(';'))),list(map(lambda inputdate:datetime.datetime.strptime(inputdate, '%Y%m%d').strftime('%Y-%m-%d'),mydict['OrderDate'][ele].split(';')))))
            cur.executemany(insert_sql,mydata_data)
        conn.commit()
    conn.close()
    pass
    ### END SOLUTION

# step11_create_orderdetail_table('data.csv',"normalized.db")
def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,p.ProductName,od.OrderDate,p.ProductUnitPrice,od.QuantityOrdered,
round(p.ProductUnitPrice*od.QuantityOrdered,2) as Total 
FROM OrderDetail od JOIN Customer c on od.CustomerID=c.CustomerID
JOIN Product p on od.ProductID=p.ProductID WHERE Name='"""+CustomerName+"'"
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,
round(sum(p.ProductUnitPrice*od.QuantityOrdered),2) as Total 
FROM OrderDetail od JOIN Customer c on od.CustomerID=c.CustomerID
JOIN Product p on od.ProductID=p.ProductID WHERE Name='"""+CustomerName+"'"
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,
    round(sum(p.ProductUnitPrice*od.QuantityOrdered),2) as Total 
    FROM OrderDetail od 
    JOIN Customer c on od.CustomerID=c.CustomerID 
    JOIN Product p on od.ProductID=p.ProductID 
    GROUP BY od.CustomerID 
    ORDER BY -Total"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT r.Region,
    round(sum(p.ProductUnitPrice*od.QuantityOrdered),2) as Total 
    FROM OrderDetail od 
    JOIN Customer c on od.CustomerID=c.CustomerID
    JOIN Product p on od.ProductID=p.ProductID 
    JOIN Country cr on c.CountryID=cr.CountryID
    JOIN Region r on cr.RegionID=r.RegionID
    GROUP BY r.Region 
    ORDER BY -Total    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT cr.Country,
    round(sum(p.ProductUnitPrice*od.QuantityOrdered)) as CountryTotal 
    FROM OrderDetail od 
    JOIN Customer c on od.CustomerID=c.CustomerID
    JOIN Product p on od.ProductID=p.ProductID 
    JOIN Country cr on c.CountryID=cr.CountryID
    GROUP BY cr.Country 
    ORDER BY -CountryTotal"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """SELECT r.Region,cr.Country, round(sum(p.ProductUnitPrice*od.QuantityOrdered)) as CountryTotal,
    RANK () OVER ( 
            PARTITION BY r.Region
            ORDER BY -round(sum(p.ProductUnitPrice*od.QuantityOrdered))
        ) CountryRegionalRank
    FROM OrderDetail od
    JOIN Customer c on od.CustomerID=c.CustomerID
    JOIN Product p on od.ProductID=p.ProductID 
    JOIN Country cr on c.CountryID=cr.CountryID
    JOIN Region r on cr.RegionID=r.RegionID
    GROUP BY cr.Country
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """WITH CountryRegion AS(
    SELECT r.Region,cr.Country, round(sum(p.ProductUnitPrice*od.QuantityOrdered)) as CountryTotal,
    RANK() OVER ( 
            PARTITION BY r.Region
            ORDER BY -round(sum(p.ProductUnitPrice*od.QuantityOrdered))
        ) CountryRegionalRank
    FROM OrderDetail od
    JOIN Customer c on od.CustomerID=c.CustomerID
    JOIN Product p on od.ProductID=p.ProductID 
    JOIN Country cr on c.CountryID=cr.CountryID
    JOIN Region r on cr.RegionID=r.RegionID
    GROUP BY cr.Country
    )
    SELECT * from CountryRegion WHERE CountryRegionalRank=1"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """WITH customertemp as (
    SELECT 
    CASE 
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 1 AND 3 THEN 'Q1'
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 4 and 6 THEN 'Q2'
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 7 and 9 THEN 'Q3'
    ELSE 'Q4' END as Quarter,
    CAST(strftime('%Y', od.OrderDate) as INTEGER) as Year,od.CustomerID,round(sum(p.ProductUnitPrice*od.QuantityOrdered)) as Total
    FROM OrderDetail od
    JOIN Product p on od.ProductID = p.ProductID
    GROUP BY Quarter,Year,od.CustomerID
    )
    SELECT *
    FROM customertemp
    ORDER BY Year"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """WITH customertemp as (
    SELECT 
    CASE 
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 1 AND 3 THEN 'Q1'
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 4 and 6 THEN 'Q2'
    WHEN cast(strftime('%m', od.OrderDate) as integer) BETWEEN 7 and 9 THEN 'Q3'
    ELSE 'Q4' END as Quarter,
    CAST(strftime('%Y', od.OrderDate) as INTEGER) as Year,od.CustomerID,round(sum(p.ProductUnitPrice*od.QuantityOrdered)) as Total
    FROM OrderDetail od
    JOIN Product p on od.ProductID = p.ProductID
    GROUP BY Quarter,Year,od.CustomerID

    ),
    ranktemp as (
    SELECT *,
    RANK() OVER ( 
                PARTITION BY ct.Quarter,ct.Year
                ORDER BY ct.Total DESC
            ) CustomerRank
            FROM customertemp as ct
    )
    SELECT *
    FROM ranktemp 
    WHERE ranktemp.CustomerRank<6
    ORDER BY Year"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """WITH customertemp as (
    SELECT 
    strftime('%m', OrderDate) as m,OrderDate,sum(round(p.ProductUnitPrice*od.QuantityOrdered)) as Total
    FROM OrderDetail od
    JOIN Product p on od.ProductID = p.ProductID
    GROUP BY m
    ),
    ranktemp as (
    SELECT CASE
            WHEN strftime('%m', ct.OrderDate) = '01' THEN 'January'
            WHEN strftime('%m', ct.OrderDate) = '02' THEN 'February'
            WHEN strftime('%m', ct.OrderDate) = '03' THEN 'March'
            WHEN strftime('%m', ct.OrderDate) = '04' THEN 'April'
            WHEN strftime('%m', ct.OrderDate) = '05' THEN 'May'
            WHEN strftime('%m', ct.OrderDate) = '06' THEN 'June'
            WHEN strftime('%m', ct.OrderDate) = '07' THEN 'July'
            WHEN strftime('%m', ct.OrderDate) = '08' THEN 'August'
            WHEN strftime('%m', ct.OrderDate) = '09' THEN 'September'
            WHEN strftime('%m', ct.OrderDate) = '10' THEN 'October'
            WHEN strftime('%m', ct.OrderDate) = '11' THEN 'November'
            WHEN strftime('%m', ct.OrderDate) = '12' THEN 'December'
        END Month,ct.Total,
    RANK() OVER ( 
                ORDER BY ct.Total DESC
            ) TotalRank
            FROM customertemp as ct
    )
    SELECT *
    FROM ranktemp """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """ WITH lagtemp AS(
    SELECT c.CustomerID,c.FirstName,c.LastName,cr.Country,od.OrderDate,
    lag(od.OrderDate, 1) over (order by c.CustomerID,od.OrderDate) as PreviousOrderDate
    FROM OrderDetail od 
    JOIN Customer c on od.CustomerID=c.CustomerID
    JOIN Country cr on c.CountryID=cr.CountryID
    ), orderdatetemp as(
    SELECT *,max(julianday(lt.OrderDate) - julianday(lt.PreviousOrderDate)) as MaxDaysWithoutOrder 
    FROM lagtemp as lt
    GROUP BY CustomerID
    )
    SELECT * FROM orderdatetemp ORDER BY MaxDaysWithoutOrder DESC,FirstName DESC """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement