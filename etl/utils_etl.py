from sqlalchemy import Engine, text
from datetime import date
import pandas as pd

def check_new_data(source_conn: Engine, etl_conn: Engine) -> bool:
    
    try:
        # Obtener la última fecha de orden en la fuente (SQL Server)
        source_query = text('''
            SELECT MAX(OrderDate) as max_order_date 
            FROM Sales.SalesOrderHeader
        ''')
        
        # Obtener la última fecha cargada en el destino (PostgreSQL)
        dest_query = text('''
            SELECT MAX(order_date) as last_loaded_date 
            FROM fact_internet_sales 
            UNION ALL
            SELECT MAX(order_date) as last_loaded_date 
            FROM fact_reseller_sales
        ''')
        
        with source_conn.connect() as source_con:
            source_result = source_con.execute(source_query)
            max_source_date = source_result.fetchone()[0]
        
        with etl_conn.connect() as dest_con:
            try:
                dest_result = dest_con.execute(dest_query)
                dest_dates = [row[0] for row in dest_result if row[0] is not None]
                max_dest_date = max(dest_dates) if dest_dates else None
            except Exception:
                # Si las tablas no existen, hay que cargar todo
                max_dest_date = None
        
        # Si no hay datos en destino, hay que cargar
        if max_dest_date is None:
            print("Primera carga: No hay datos en el destino")
            return True
        
        # Si la fecha máxima en fuente es mayor que en destino, hay nuevos datos
        if max_source_date and max_source_date > max_dest_date:
            print(f"Hay nuevos datos: Fuente={max_source_date}, Destino={max_dest_date}")
            return True
        else:
            print(f"No hay datos nuevos. Última fecha en fuente: {max_source_date}")
            return False
            
    except Exception as e:
        print(f'[Error] Verificando nuevos datos: {e}')
        # En caso de error, asumir que hay que cargar
        return True

def check_table_exists(etl_conn: Engine, table_name: str) -> bool:
    
    try:
        query = text('''
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            )
        ''')
        with etl_conn.connect() as conn:
            result = conn.execute(query, {'table_name': table_name})
            exists = result.scalar()
            return exists
    except Exception as e:
        print(f'[Error] Verificando tabla {table_name}: {e}')
        return False

def push_dimensions(source_conn: Engine, etl_conn: Engine, replace: bool = False):
   
    # Importar módulos (evitar circular imports)
    from src import extract, transform, load
    
    print("Iniciando carga de dimensiones...")
    
    try:
        # Extraer datos de dimensiones desde SQL Server
        print("Extrayendo datos de dimensiones...")
        dim_customer = extract.extract_customers(source_conn)
        dim_product = extract.extract_products(source_conn)
        dim_territory = extract.extract_sales_territory(source_conn)
        dim_currency = extract.extract_currency(source_conn)
        dim_employee = extract.extract_employees(source_conn)
        dim_reseller = extract.extract_stores(source_conn)
        sales_reason = extract.extract_sales_reason(source_conn)
        
        # Transformar dimensiones
        print("Transformando dimensiones...")
        dim_customer_transformed = transform.transform_customer(dim_customer)
        dim_product_transformed = transform.transform_product(dim_product)
        dim_date_transformed = transform.transform_date()  # Dimensión de tiempo generada
        dim_territory_transformed = transform.transform_territory(dim_territory)
        dim_currency_transformed = transform.transform_currency(dim_currency)
        dim_employee_transformed = transform.transform_employee(dim_employee)
        dim_reseller_transformed = transform.transform_reseller(dim_reseller)
        sales_reason_transformed = transform.transform_sales_reason(sales_reason)
        
        # Validar transformaciones
        print("Validando transformaciones...")
        transform.validate_transformations(dim_customer_transformed, 'dim_customer')
        transform.validate_transformations(dim_product_transformed, 'dim_product')
        transform.validate_transformations(dim_territory_transformed, 'dim_territory')
        
        # Cargar dimensiones a PostgreSQL
        print("Cargando dimensiones a la bodega...")
        load.load(dim_customer_transformed, etl_conn, 'dim_customer', replace)
        load.load(dim_product_transformed, etl_conn, 'dim_product', replace)
        load.load(dim_date_transformed, etl_conn, 'dim_date', replace)
        load.load(dim_territory_transformed, etl_conn, 'dim_territory', replace)
        load.load(dim_currency_transformed, etl_conn, 'dim_currency', replace)
        load.load(dim_employee_transformed, etl_conn, 'dim_employee', replace)
        load.load(dim_reseller_transformed, etl_conn, 'dim_reseller', replace)
        load.load(sales_reason_transformed, etl_conn, 'dim_sales_reason', replace)
        
        print("✓ Todas las dimensiones cargadas exitosamente")
        
    except Exception as e:
        print(f"✗ Error cargando dimensiones: {e}")
        raise

def push_facts(source_conn: Engine, etl_conn: Engine, incremental: bool = True):
    
    # Importar módulos
    from src import extract, transform, load
    
    print("Iniciando carga de hechos...")
    
    try:
        # Extraer dimensiones existentes para las transformaciones
        print("Extrayendo dimensiones para transformaciones...")
        dimensions = extract.extract_dimensions_from_dw(etl_conn)
        
        # Extraer datos de hechos desde SQL Server
        print("Extrayendo datos de ventas...")
        internet_sales = extract.extract_internet_sales(source_conn)
        reseller_sales = extract.extract_reseller_sales(source_conn)
        
        # Transformar hechos
        print("Transformando hechos...")
        fact_internet_sales = transform.transform_internet_sales(internet_sales, dimensions)
        fact_reseller_sales = transform.transform_reseller_sales(reseller_sales, dimensions)
        
        # Cargar hechos a PostgreSQL
        print("Cargando hechos a la bodega...")
        
        if incremental:
            # Carga incremental
            load.load_incremental_fact_internet_sales(fact_internet_sales, etl_conn)
            load.load_incremental_fact_reseller_sales(fact_reseller_sales, etl_conn)
        else:
            # Carga completa
            load.load(fact_internet_sales, etl_conn, 'fact_internet_sales', replace=True)
            load.load(fact_reseller_sales, etl_conn, 'fact_reseller_sales', replace=True)
        
        print("✓ Todos los hechos cargados exitosamente")
        
    except Exception as e:
        print(f"✗ Error cargando hechos: {e}")
        raise

def get_etl_status(etl_conn: Engine) -> dict:
    
    status = {}
    
    try:
        tables = [
            'dim_customer', 'dim_product', 'dim_date', 'dim_territory',
            'dim_currency', 'dim_employee', 'dim_reseller', 'dim_sales_reason',
            'fact_internet_sales', 'fact_reseller_sales'
        ]
        
        with etl_conn.connect() as conn:
            for table in tables:
                if check_table_exists(etl_conn, table):
                    query = text(f'SELECT COUNT(*) as count FROM {table}')
                    result = conn.execute(query)
                    count = result.scalar()
                    status[table] = count
                else:
                    status[table] = 'Tabla no existe'
        
        return status
        
    except Exception as e:
        print(f'[Error] Obteniendo estado ETL: {e}')
        return {}

def log_etl_run(etl_conn: Engine, process_name: str, status: str, records_processed: int = 0):
    
    try:
        # Crear tabla de logs si no existe
        create_log_table = text('''
            CREATE TABLE IF NOT EXISTS etl_log (
                log_id SERIAL PRIMARY KEY,
                process_name VARCHAR(100),
                run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(50),
                records_processed INTEGER,
                details TEXT
            )
        ''')
        
        insert_log = text('''
            INSERT INTO etl_log (process_name, status, records_processed)
            VALUES (:process_name, :status, :records_processed)
        ''')
        
        with etl_conn.connect() as conn:
            conn.execute(create_log_table)
            conn.execute(insert_log, {
                'process_name': process_name,
                'status': status,
                'records_processed': records_processed
            })
            conn.commit()
            
        print(f"✓ Log registrado: {process_name} - {status}")
        
    except Exception as e:
        print(f'[Error] Registrando log ETL: {e}')