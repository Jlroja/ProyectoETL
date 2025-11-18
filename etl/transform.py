import datetime
from datetime import timedelta, date, datetime
from typing import Tuple, Any, List
import numpy as np
import pandas as pd
from pandas import DataFrame


def transform_customer(customer_data: DataFrame) -> DataFrame:
   
    df = customer_data.copy()
    
    # Limpieza de datos
    df.replace({'': 'No especificado', np.nan: 'No especificado'}, inplace=True)
    
    # Crear nombre completo
    df['customer_name'] = df['FirstName'] + ' ' + df['LastName']
    
    # Determinar tipo de cliente
    df['customer_type'] = df['StoreID'].apply(
        lambda x: 'Business' if pd.notna(x) else 'Individual'
    )
    
    # Clasificación por email promotion
    df['email_promotion_category'] = df['EmailPromotion'].apply(
        lambda x: 'Alta' if x == 2 else 'Media' if x == 1 else 'Baja'
    )
    
    # Calcular edad si hay fecha de nacimiento (no disponible en AdventureWorks directamente)
    # En su lugar, usar otros campos para segmentación
    
    df["saved_date"] = date.today()
    
    # Seleccionar y renombrar columnas finales
    dim_customer = df[[
        'CustomerID', 'PersonID', 'StoreID', 'customer_name', 
        'EmailAddress', 'PhoneNumber', 'City', 'StateProvince', 
        'CountryRegion', 'customer_type', 'email_promotion_category', 
        'saved_date'
    ]].rename(columns={
        'CustomerID': 'customer_id',
        'PersonID': 'person_id',
        'StoreID': 'store_id',
        'EmailAddress': 'email',
        'PhoneNumber': 'phone',
        'City': 'city',
        'StateProvince': 'state_province',
        'CountryRegion': 'country_region'
    })
    
    return dim_customer


def transform_product(product_data: DataFrame) -> DataFrame:
   
    df = product_data.copy()
    
    # Limpieza de datos
    df.replace({'': 'No especificado', np.nan: 'No especificado'}, inplace=True)
    
    # Calcular margen de ganancia
    df['profit_margin'] = ((df['ListPrice'] - df['StandardCost']) / df['ListPrice'] * 100).round(2)
    df['profit_margin'].fillna(0, inplace=True)
    
    # Categorizar productos por precio
    df['price_category'] = pd.cut(
        df['ListPrice'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['Económico', 'Estándar', 'Premium', 'Lujo']
    )
    
    # Categorizar por margen de ganancia
    df['margin_category'] = pd.cut(
        df['profit_margin'],
        bins=[-float('inf'), 0, 20, 40, float('inf')],
        labels=['Pérdida', 'Bajo', 'Medio', 'Alto']
    )
    
    # Crear categoría completa
    df['full_category'] = df['CategoryName'] + ' - ' + df['SubcategoryName']
    
    df["saved_date"] = date.today()
    
    dim_product = df[[
        'ProductID', 'ProductName', 'ProductNumber', 'Color', 'Size', 'Weight',
        'StandardCost', 'ListPrice', 'profit_margin', 'price_category',
        'margin_category', 'CategoryName', 'SubcategoryName', 'full_category',
        'ProductModelName', 'saved_date'
    ]].rename(columns={
        'ProductID': 'product_id',
        'ProductName': 'product_name',
        'ProductNumber': 'product_number',
        'CategoryName': 'category_name',
        'SubcategoryName': 'subcategory_name',
        'ProductModelName': 'product_model_name'
    })
    
    return dim_product


def transform_date() -> DataFrame:
    
    dim_date = pd.DataFrame({
        "date": pd.date_range(start='2005-01-01', end='2014-12-31', freq='D')
    })
    
    # Atributos básicos de fecha
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["weekday"] = dim_date["date"].dt.weekday
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["day_of_year"] = dim_date["date"].dt.day_of_year
    
    # Nombres
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["day_name"] = dim_date["date"].dt.day_name()
    
    # Semana del año
    dim_date["week_of_year"] = dim_date["date"].dt.isocalendar().week
    
    # Flags importantes
    dim_date["is_weekend"] = dim_date["weekday"].apply(lambda x: x >= 5)
    dim_date["is_month_end"] = dim_date["date"].dt.is_month_end
    dim_date["is_quarter_end"] = dim_date["date"].dt.is_quarter_end
    dim_date["is_year_end"] = dim_date["date"].dt.is_year_end
    
    # Trimestre fiscal (asumiendo año fiscal = año calendario)
    dim_date["fiscal_quarter"] = dim_date["quarter"]
    dim_date["fiscal_year"] = dim_date["year"]
    
    dim_date["saved_date"] = date.today()
    
    return dim_date


def transform_territory(territory_data: DataFrame) -> DataFrame:
    
    df = territory_data.copy()
    
    df.rename(columns={
        'TerritoryID': 'territory_id',
        'Name': 'territory_name',
        'CountryRegionCode': 'country_region_code',
        'Group': 'region_group',
        'SalesYTD': 'sales_ytd',
        'SalesLastYear': 'sales_last_year',
        'CostYTD': 'cost_ytd',
        'CostLastYear': 'cost_last_year'
    }, inplace=True)
    
    # Calcular métricas de performance
    df['ytd_profit'] = df['sales_ytd'] - df['cost_ytd']
    df['last_year_profit'] = df['sales_last_year'] - df['cost_last_year']
    df['sales_growth'] = ((df['sales_ytd'] - df['sales_last_year']) / df['sales_last_year'] * 100).round(2)
    
    df["saved_date"] = date.today()
    
    return df


def transform_employee(employee_data: DataFrame) -> DataFrame:
   
    df = employee_data.copy()
    
    # Crear nombre completo
    df['employee_name'] = df['FirstName'] + ' ' + df['LastName']
    
    # Calcular edad y antigüedad
    df['age'] = (date.today() - df['BirthDate'].dt.date).dt.days // 365
    df['years_of_service'] = (date.today() - df['HireDate'].dt.date).dt.days // 365
    
    # Categorizar por departamento
    df['department_category'] = df['DepartmentName'].apply(
        lambda x: 'Ventas' if 'Sales' in str(x) else 'Administrativo' if 'Executive' in str(x) else 'Operaciones'
    )
    
    df.rename(columns={
        'BusinessEntityID': 'business_entity_id',
        'JobTitle': 'job_title',
        'BirthDate': 'birth_date',
        'HireDate': 'hire_date',
        'DepartmentName': 'department_name'
    }, inplace=True)
    
    df["saved_date"] = date.today()
    
    return df[[
        'business_entity_id', 'employee_name', 'job_title', 'department_name',
        'department_category', 'age', 'years_of_service', 'birth_date', 
        'hire_date', 'saved_date'
    ]]


def transform_reseller(store_data: DataFrame) -> DataFrame:
    
    df = store_data.copy()
    
    df.rename(columns={
        'StoreID': 'store_id',
        'StoreName': 'store_name',
        'City': 'city',
        'StateProvince': 'state_province',
        'CountryRegion': 'country_region'
    }, inplace=True)
    
    # Categorizar por ubicación
    df['region'] = df['state_province'].apply(
        lambda x: 'Norte' if 'North' in str(x) else 'Sur' if 'South' in str(x) else 'Este' if 'East' in str(x) else 'Oeste' if 'West' in str(x) else 'Central'
    )
    
    df["saved_date"] = date.today()
    
    return df


def transform_currency(currency_data: DataFrame) -> DataFrame:
   
    df = currency_data.copy()
    
    df.rename(columns={
        'CurrencyCode': 'currency_code',
        'Name': 'currency_name'
    }, inplace=True)
    
    df["saved_date"] = date.today()
    
    return df


def transform_internet_sales(sales_data: DataFrame, dimensions: dict) -> DataFrame:
    
    df = sales_data.copy()
    
    # Unir con dimensiones
    df = df.merge(
        dimensions['dim_customer'][['customer_key', 'customer_id']], 
        left_on='CustomerID', right_on='customer_id', how='left'
    )
    
    df = df.merge(
        dimensions['dim_product'][['product_key', 'product_id']], 
        left_on='ProductID', right_on='product_id', how='left'
    )
    
    # Convertir fechas y unir con dim_date
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df = df.merge(
        dimensions['dim_date'][['date_key', 'date']], 
        left_on='OrderDate', right_on='date', how='left'
    )
    
    # Calcular métricas adicionales
    df['discount_amount'] = df['UnitPriceDiscount'] * df['OrderQty'] * df['UnitPrice']
    df['net_sales_amount'] = df['LineTotal'] - df['discount_amount']
    df['profit'] = df['net_sales_amount'] - (df['StandardCost'] * df['OrderQty'])
    
    # Crear fact table
    fact_internet_sales = df[[
        'SalesOrderID', 'SalesOrderDetailID', 'customer_key', 'product_key', 
        'date_key', 'OrderQty', 'UnitPrice', 'LineTotal', 'discount_amount',
        'net_sales_amount', 'profit', 'TaxAmt', 'Freight'
    ]].rename(columns={
        'SalesOrderID': 'sales_order_id',
        'SalesOrderDetailID': 'sales_order_detail_id',
        'OrderQty': 'order_quantity',
        'UnitPrice': 'unit_price',
        'LineTotal': 'line_total',
        'TaxAmt': 'tax_amount',
        'Freight': 'freight_amount'
    })
    
    fact_internet_sales["saved_date"] = date.today()
    
    return fact_internet_sales


def transform_reseller_sales(sales_data: DataFrame, dimensions: dict) -> DataFrame:
    
    df = sales_data.copy()
    
    # Unir con dimensiones
    df = df.merge(
        dimensions['dim_reseller'][['reseller_key', 'store_id']], 
        left_on='StoreID', right_on='store_id', how='left'
    )
    
    df = df.merge(
        dimensions['dim_product'][['product_key', 'product_id']], 
        left_on='ProductID', right_on='product_id', how='left'
    )
    
    df = df.merge(
        dimensions['dim_employee'][['employee_key', 'business_entity_id']], 
        left_on='SalesPersonID', right_on='business_entity_id', how='left'
    )
    
    # Convertir fechas y unir con dim_date
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df = df.merge(
        dimensions['dim_date'][['date_key', 'date']], 
        left_on='OrderDate', right_on='date', how='left'
    )
    
    # Calcular métricas adicionales
    df['discount_amount'] = df['UnitPriceDiscount'] * df['OrderQty'] * df['UnitPrice']
    df['net_sales_amount'] = df['LineTotal'] - df['discount_amount']
    df['profit'] = df['net_sales_amount'] - (df['StandardCost'] * df['OrderQty'])
    
    # Crear fact table
    fact_reseller_sales = df[[
        'SalesOrderID', 'SalesOrderDetailID', 'reseller_key', 'product_key', 
        'employee_key', 'date_key', 'OrderQty', 'UnitPrice', 'LineTotal', 
        'discount_amount', 'net_sales_amount', 'profit', 'TaxAmt', 'Freight'
    ]].rename(columns={
        'SalesOrderID': 'sales_order_id',
        'SalesOrderDetailID': 'sales_order_detail_id',
        'OrderQty': 'order_quantity',
        'UnitPrice': 'unit_price',
        'LineTotal': 'line_total',
        'TaxAmt': 'tax_amount',
        'Freight': 'freight_amount'
    })
    
    fact_reseller_sales["saved_date"] = date.today()
    
    return fact_reseller_sales


def transform_sales_reason(sales_reason_data: DataFrame) -> DataFrame:
    
    df = sales_reason_data.copy()
    
    df.rename(columns={
        'SalesReasonID': 'sales_reason_id',
        'ReasonName': 'reason_name',
        'ReasonType': 'reason_type',
        'SalesOrderID': 'sales_order_id'
    }, inplace=True)
    
    df["saved_date"] = date.today()
    
    return df


def calculate_sales_metrics(fact_table: DataFrame) -> DataFrame:
    
    metrics = fact_table.groupby(['date_key', 'product_key']).agg({
        'order_quantity': 'sum',
        'line_total': 'sum',
        'discount_amount': 'sum',
        'net_sales_amount': 'sum',
        'profit': 'sum'
    }).reset_index()
    
    metrics['avg_sale_amount'] = metrics['line_total'] / metrics['order_quantity']
    metrics['discount_rate'] = (metrics['discount_amount'] / metrics['line_total'] * 100).round(2)
    metrics['profit_margin'] = (metrics['profit'] / metrics['net_sales_amount'] * 100).round(2)
    
    return metrics


def validate_transformations(df: DataFrame, table_name: str) -> bool:
   
    try:
        # Verificar que no hay valores nulos en campos críticos
        critical_columns = [col for col in df.columns if 'key' in col or 'id' in col]
        for col in critical_columns:
            if df[col].isnull().any():
                print(f"Advertencia: Valores nulos en {col} para {table_name}")
                return False
        
        # Verificar que no hay duplicados en claves primarias
        if 'customer_key' in df.columns:
            if df['customer_key'].duplicated().any():
                print(f"Advertencia: Duplicados en customer_key para {table_name}")
                return False
        
        print(f"✓ Transformación validada para {table_name}")
        return True
        
    except Exception as e:
        print(f"✗ Error validando {table_name}: {e}")
        return False