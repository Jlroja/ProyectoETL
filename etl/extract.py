import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables: list, connection: Engine) -> list[pd.DataFrame]:
    
    dataframes = []
    for table in tables:
        df = pd.read_sql_table(table, connection)
        dataframes.append(df)
    return dataframes


def extract_internet_sales(connection: Engine, start_date: str = '2011-01-01'):
    """
    Extraemos datos de ventas por internet de AdventureWorks
    """
    query = """
    SELECT 
        soh.SalesOrderID,
        soh.OrderDate,
        soh.DueDate,
        soh.ShipDate,
        soh.CustomerID,
        soh.SalesPersonID,
        soh.TerritoryID,
        soh.SubTotal,
        soh.TaxAmt,
        soh.Freight,
        soh.TotalDue,
        sod.SalesOrderDetailID,
        sod.ProductID,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal,
        c.PersonID as CustomerPersonID,
        soh.OnlineOrderFlag
    FROM Sales.SalesOrderHeader soh
    JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
    WHERE soh.OnlineOrderFlag = 1
    AND soh.OrderDate >= ?
    """
    return pd.read_sql_query(query, connection, params=[start_date])


def extract_reseller_sales(connection: Engine, start_date: str = '2011-01-01'):
    """
    Extraemos datos de ventas por revendedores de AdventureWorks
    """
    query = """
    SELECT 
        soh.SalesOrderID,
        soh.OrderDate,
        soh.DueDate,
        soh.ShipDate,
        soh.CustomerID,
        soh.SalesPersonID,
        soh.TerritoryID,
        soh.SubTotal,
        soh.TaxAmt,
        soh.Freight,
        soh.TotalDue,
        sod.SalesOrderDetailID,
        sod.ProductID,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal,
        s.BusinessEntityID as StoreID,
        s.Name as StoreName,
        soh.OnlineOrderFlag
    FROM Sales.SalesOrderHeader soh
    JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
    JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
    WHERE soh.OnlineOrderFlag = 0
    AND soh.OrderDate >= ?
    """
    return pd.read_sql_query(query, connection, params=[start_date])


def extract_customers(connection: Engine):
    """
    Extraemos datos de clientes
    """
    query = """
    SELECT 
        c.CustomerID,
        c.PersonID,
        c.StoreID,
        p.FirstName,
        p.LastName,
        p.EmailPromotion,
        be.EmailAddress,
        pp.PhoneNumber,
        a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name as StateProvince,
        cr.Name as CountryRegion
    FROM Sales.Customer c
    LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Person.EmailAddress be ON p.BusinessEntityID = be.BusinessEntityID
    LEFT JOIN Person.PersonPhone pp ON p.BusinessEntityID = pp.BusinessEntityID
    LEFT JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
    LEFT JOIN Person.Address a ON bea.AddressID = a.AddressID
    LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
    LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
    """
    return pd.read_sql_query(query, connection)


def extract_products(connection: Engine):
    """
    Extraemos datos de productos
    """
    query = """
    SELECT 
        p.ProductID,
        p.Name as ProductName,
        p.ProductNumber,
        p.Color,
        p.StandardCost,
        p.ListPrice,
        p.Size,
        p.Weight,
        p.ProductLine,
        p.Class,
        p.Style,
        psc.Name as SubcategoryName,
        pc.Name as CategoryName,
        pm.Name as ProductModelName
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
    LEFT JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
    """
    return pd.read_sql_query(query, connection)


def extract_sales_territory(connection: Engine):
    """
    Extraemos datos de territorios de venta
    """
    return pd.read_sql_table('SalesTerritory', connection, schema='Sales')


def extract_currency(connection: Engine):
    """
    Extraemos datos de monedas
    """
    return pd.read_sql_table('Currency', connection, schema='Sales')


def extract_employees(connection: Engine):
    """
    Extraemos datos de empleados/vendedores
    """
    query = """
    SELECT 
        e.BusinessEntityID,
        p.FirstName,
        p.LastName,
        e.JobTitle,
        e.HireDate,
        e.BirthDate,
        d.Name as DepartmentName
    FROM HumanResources.Employee e
    JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    JOIN HumanResources.EmployeeDepartmentHistory edh ON e.BusinessEntityID = edh.BusinessEntityID
    JOIN HumanResources.Department d ON edh.DepartmentID = d.DepartmentID
    WHERE edh.EndDate IS NULL  -- Departamento actual
    """
    return pd.read_sql_query(query, connection)


def extract_stores(connection: Engine):
    """
    Extraemos datos de tiendas/revendedores
    """
    query = """
    SELECT 
        s.BusinessEntityID as StoreID,
        s.Name as StoreName,
        bea.AddressID,
        a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name as StateProvince,
        cr.Name as CountryRegion
    FROM Sales.Store s
    JOIN Person.BusinessEntityAddress bea ON s.BusinessEntityID = bea.BusinessEntityID
    JOIN Person.Address a ON bea.AddressID = a.AddressID
    JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
    JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
    """
    return pd.read_sql_query(query, connection)


def extract_sales_person(connection: Engine):
    """
    Extraemos datos de vendedores
    """
    query = """
    SELECT 
        sp.BusinessEntityID,
        sp.TerritoryID,
        sp.SalesQuota,
        sp.Bonus,
        sp.CommissionPct,
        sp.SalesYTD,
        sp.SalesLastYear
    FROM Sales.SalesPerson sp
    """
    return pd.read_sql_query(query, connection)


def extract_hecho_internet_sales(etl_connection: Engine):
    """
    Extraemos los datos ya transformados para el hecho de ventas por internet
    (Para cargas incrementales desde la bodega)
    """
    df_trans = pd.read_sql_table('trans_internet_sales', etl_connection)
    dim_customer = pd.read_sql_table('dim_customer', etl_connection)
    dim_product = pd.read_sql_table('dim_product', etl_connection)
    dim_date = pd.read_sql_table('dim_date', etl_connection)
    dim_territory = pd.read_sql_table('dim_territory', etl_connection)
    
    return [df_trans, dim_customer, dim_product, dim_date, dim_territory]


def extract_hecho_reseller_sales(etl_connection: Engine):
    """
    Extraemos los datos ya transformados para el hecho de ventas por revendedores
    (Para cargas incrementales desde la bodega)
    """
    df_trans = pd.read_sql_table('trans_reseller_sales', etl_connection)
    dim_reseller = pd.read_sql_table('dim_reseller', etl_connection)
    dim_product = pd.read_sql_table('dim_product', etl_connection)
    dim_date = pd.read_sql_table('dim_date', etl_connection)
    dim_territory = pd.read_sql_table('dim_territory', etl_connection)
    dim_employee = pd.read_sql_table('dim_employee', etl_connection)
    
    return [df_trans, dim_reseller, dim_product, dim_date, dim_territory, dim_employee]


def extract_dimensions_from_dw(etl_connection: Engine):
    """
    Extraemos todas las dimensiones de la bodega de datos
    (Para transformaciones que necesitan referencias)
    """
    dim_customer = pd.read_sql_table('dim_customer', etl_connection)
    dim_product = pd.read_sql_table('dim_product', etl_connection)
    dim_date = pd.read_sql_table('dim_date', etl_connection)
    dim_territory = pd.read_sql_table('dim_territory', etl_connection)
    dim_currency = pd.read_sql_table('dim_currency', etl_connection)
    dim_employee = pd.read_sql_table('dim_employee', etl_connection)
    dim_reseller = pd.read_sql_table('dim_reseller', etl_connection)
    
    return {
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'dim_date': dim_date,
        'dim_territory': dim_territory,
        'dim_currency': dim_currency,
        'dim_employee': dim_employee,
        'dim_reseller': dim_reseller
    }


def extract_sales_reason(connection: Engine):
    """
    Extraemos razones de venta
    """
    query = """
    SELECT 
        sr.SalesReasonID,
        sr.Name as ReasonName,
        sr.ReasonType,
        soh.SalesOrderID
    FROM Sales.SalesReason sr
    JOIN Sales.SalesOrderHeaderSalesReason sohsr ON sr.SalesReasonID = sohsr.SalesReasonID
    JOIN Sales.SalesOrderHeader soh ON sohsr.SalesOrderID = soh.SalesOrderID
    """
    return pd.read_sql_query(query, connection)