import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert


def load_dim_customer(dim_customer: DataFrame, etl_conn: Engine):
    """Carga dimensión cliente"""
    dim_customer.to_sql('dim_customer', etl_conn, if_exists='append', index_label='customer_key')


def load_dim_product(dim_product: DataFrame, etl_conn: Engine):
    """Carga dimensión producto"""
    dim_product.to_sql('dim_product', etl_conn, if_exists='append', index_label='product_key')


def load_dim_date(dim_date: DataFrame, etl_conn: Engine):
    """Carga dimensión fecha"""
    dim_date.to_sql('dim_date', etl_conn, if_exists='append', index_label='date_key')


def load_dim_territory(dim_territory: DataFrame, etl_conn: Engine):
    """Carga dimensión territorio"""
    dim_territory.to_sql('dim_territory', etl_conn, if_exists='append', index_label='territory_key')


def load_dim_currency(dim_currency: DataFrame, etl_conn: Engine):
    """Carga dimensión moneda"""
    dim_currency.to_sql('dim_currency', etl_conn, if_exists='append', index_label='currency_key')


def load_dim_employee(dim_employee: DataFrame, etl_conn: Engine):
    """Carga dimensión empleado"""
    dim_employee.to_sql('dim_employee', etl_conn, if_exists='append', index_label='employee_key')


def load_dim_reseller(dim_reseller: DataFrame, etl_conn: Engine):
    """Carga dimensión revendedor"""
    dim_reseller.to_sql('dim_reseller', etl_conn, if_exists='append', index_label='reseller_key')


def load_fact_internet_sales(fact_internet_sales: DataFrame, etl_conn: Engine):
    """Carga hecho ventas por internet"""
    fact_internet_sales.to_sql('fact_internet_sales', etl_conn, if_exists='append', index=False)


def load_fact_reseller_sales(fact_reseller_sales: DataFrame, etl_conn: Engine):
    """Carga hecho ventas por revendedores"""
    fact_reseller_sales.to_sql('fact_reseller_sales', etl_conn, if_exists='append', index=False)


def load_trans_internet_sales(trans_internet_sales: DataFrame, etl_conn: Engine):
    """Carga datos transformados de ventas por internet"""
    trans_internet_sales.to_sql('trans_internet_sales', etl_conn, if_exists='append', index_label='trans_internet_key')


def load_trans_reseller_sales(trans_reseller_sales: DataFrame, etl_conn: Engine):
    """Carga datos transformados de ventas por revendedores"""
    trans_reseller_sales.to_sql('trans_reseller_sales', etl_conn, if_exists='append', index_label='trans_reseller_key')


def load_sales_reason(sales_reason: DataFrame, etl_conn: Engine):
    """Carga dimensión razón de venta"""
    sales_reason.to_sql('dim_sales_reason', etl_conn, if_exists='append', index_label='sales_reason_key')


def load_incremental_fact_internet_sales(fact_data: DataFrame, etl_conn: Engine):
    """
    Carga incremental para fact_internet_sales usando UPSERT
    """
    # Obtener máximo SalesOrderID existente para carga incremental
    try:
        max_order_query = "SELECT MAX(sales_order_id) as max_id FROM fact_internet_sales"
        max_order_id = pd.read_sql_query(max_order_query, etl_conn).iloc[0, 0]
        
        if max_order_id is not None:
            # Filtrar solo registros nuevos
            fact_data = fact_data[fact_data['sales_order_id'] > max_order_id]
    except:
        # La tabla no existe, cargar todos los datos
        pass
    
    if len(fact_data) > 0:
        fact_data.to_sql('fact_internet_sales', etl_conn, if_exists='append', index=False)
        print(f"Cargadas {len(fact_data)} nuevas filas en fact_internet_sales")
    else:
        print("No hay nuevos datos para fact_internet_sales")


def load_incremental_fact_reseller_sales(fact_data: DataFrame, etl_conn: Engine):
    """
    Carga incremental para fact_reseller_sales usando UPSERT
    """
    # Obtener máximo SalesOrderID existente para carga incremental
    try:
        max_order_query = "SELECT MAX(sales_order_id) as max_id FROM fact_reseller_sales"
        max_order_id = pd.read_sql_query(max_order_query, etl_conn).iloc[0, 0]
        
        if max_order_id is not None:
            # Filtrar solo registros nuevos
            fact_data = fact_data[fact_data['sales_order_id'] > max_order_id]
    except:
        # La tabla no existe, cargar todos los datos
        pass
    
    if len(fact_data) > 0:
        fact_data.to_sql('fact_reseller_sales', etl_conn, if_exists='append', index=False)
        print(f"Cargadas {len(fact_data)} nuevas filas en fact_reseller_sales")
    else:
        print("No hay nuevos datos para fact_reseller_sales")


def load_with_upsert(table: DataFrame, etl_conn: Engine, table_name: str, conflict_columns: list):
    """
    Carga datos con estrategia UPSERT para evitar duplicados
    """
    if table.empty:
        print(f"Tabla {table_name} vacía, no hay datos para cargar")
        return
    
    # Crear statement de UPSERT
    stmt = insert(table).values(table.to_dict('records'))
    
    update_dict = {col: getattr(stmt.excluded, col) for col in table.columns if col not in conflict_columns}
    
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_dict
    )
    
    with etl_conn.connect() as conn:
        conn.execute(stmt)
        conn.commit()
    
    print(f"Datos cargados en {table_name} con UPSERT")


def load(table: DataFrame, etl_conn: Engine, table_name: str, replace: bool = False):
  
    if table.empty:
        print(f"DataFrame vacío para {table_name}, omitiendo carga")
        return
        
    if replace:
        with etl_conn.connect() as conn:
            conn.execute(text(f'DELETE FROM {table_name}'))
            conn.commit()
        table.to_sql(table_name, etl_conn, if_exists='append', index=False)
        print(f"Tabla {table_name} reemplazada con {len(table)} registros")
    else:
        table.to_sql(table_name, etl_conn, if_exists='append', index=False)
        print(f"Datos cargados en {table_name}: {len(table)} registros")


def load_all_dimensions(dimensions_dict: dict, etl_conn: Engine, replace: bool = False):
    
    for dim_name, dim_data in dimensions_dict.items():
        if not dim_data.empty:
            load(dim_data, etl_conn, dim_name, replace)
            print(f"Dimensión {dim_name} cargada: {len(dim_data)} registros")
        else:
            print(f"Dimensión {dim_name} vacía, omitiendo carga")


def validate_load(etl_conn: Engine, table_name: str):
    
    try:
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = pd.read_sql_query(count_query, etl_conn)
        count = result.iloc[0, 0]
        print(f"Validación {table_name}: {count} registros cargados")
        return count
    except Exception as e:
        print(f"Error validando {table_name}: {e}")
        return 0