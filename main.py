import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from src import extract, transform, load, utils_etl
import psycopg2
import sys
import os

# Agregar el directorio src al path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

def main():
    """
    Función principal del ETL para AdventureWorks
    """
    print("ETL AdventureWorks - Iniciando")
    
    # Cargar configuración
    try:
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)
            config_source = config['SOURCE_DB']  # SQL Server
            config_target = config['TARGET_DB']  # PostgreSQL
            etl_settings = config['ETL_SETTINGS']
    except FileNotFoundError:
        print("Error: Archivo config.yml no encontrado")
        return
    except KeyError as e:
        print(f"Error: Configuración faltante en config.yml: {e}")
        return

    # Construir URLs de conexión
    try:
        # Conexión a SQL Server (fuente - AdventureWorks)
        source_conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config_source['host']},{config_source['port']};"
            f"DATABASE={config_source['dbname']};"
            f"UID={config_source['user']};"
            f"PWD={config_source['password']}"
        )
        source_conn = create_engine(f"mssql+pyodbc:///?odbc_connect={source_conn_string}")
        
        # Conexión a PostgreSQL (destino - Data Warehouse)
        target_url = (
            f"{config_target['drivername']}://{config_target['user']}:{config_target['password']}"
            f"@{config_target['host']}:{config_target['port']}/{config_target['dbname']}"
        )
        target_conn = create_engine(target_url)
        
        print("✓ Conexiones a bases de datos establecidas")
        
    except Exception as e:
        print(f"✗ Error conectando a bases de datos: {e}")
        return

    # Verificar si existe la estructura de la bodega
    inspector = inspect(target_conn)
    existing_tables = inspector.get_table_names()
    
    # Crear estructura si no existe
    if not existing_tables:
        print("Creando estructura de la bodega de datos...")
        try:
            conn = psycopg2.connect(
                dbname=config_target['dbname'],
                user=config_target['user'],
                password=config_target['password'],
                host=config_target['host'],
                port=config_target['port']
            )
            cur = conn.cursor()
            
            # Ejecutar scripts DDL
            with open('sqlscripts.yml', 'r') as f:
                sql_scripts = yaml.safe_load(f)
                for table_name, ddl in sql_scripts.items():
                    print(f"Creando tabla: {table_name}")
                    cur.execute(ddl)
                    conn.commit()
            
            cur.close()
            conn.close()
            print("✓ Estructura de bodega creada exitosamente")
            
        except Exception as e:
            print(f"✗ Error creando estructura: {e}")
            return

    # Verificar si hay nuevos datos para procesar
    if utils_etl.check_new_data(source_conn, target_conn):
        print("Nuevos datos detectados, iniciando procesamiento ETL...")
        
        # Obtener estado actual del ETL
        status_before = utils_etl.get_etl_status(target_conn)
        print("Estado inicial del ETL:", status_before)
        
        # CARGAR DIMENSIONES
        if etl_settings.get('load_dimensions', True) or not utils_etl.check_table_exists(target_conn, 'dim_customer'):
            print("\n--- CARGANDO DIMENSIONES ---")
            try:
                utils_etl.push_dimensions(
                    source_conn, 
                    target_conn, 
                    replace=etl_settings.get('replace_dimensions', False)
                )
                utils_etl.log_etl_run(target_conn, 'Dimensiones', 'Exitoso')
            except Exception as e:
                print(f"✗ Error cargando dimensiones: {e}")
                utils_etl.log_etl_run(target_conn, 'Dimensiones', 'Fallido')
                return
        else:
            print("✓ Dimensiones ya cargadas, omitiendo...")
        
        # CARGAR HECHOS - VENTAS POR INTERNET
        print("\n--- CARGANDO HECHOS: VENTAS POR INTERNET ---")
        try:
            # Extraer dimensiones para transformación
            dimensions = extract.extract_dimensions_from_dw(target_conn)
            
            # Extraer y transformar ventas por internet
            internet_sales = extract.extract_internet_sales(
                source_conn, 
                start_date=etl_settings.get('start_date', '2011-01-01')
            )
            fact_internet_sales = transform.transform_internet_sales(internet_sales, dimensions)
            
            # Validar transformación
            if transform.validate_transformations(fact_internet_sales, 'fact_internet_sales'):
                # Cargar datos
                if etl_settings.get('incremental_load', True):
                    load.load_incremental_fact_internet_sales(fact_internet_sales, target_conn)
                else:
                    load.load(fact_internet_sales, target_conn, 'fact_internet_sales', replace=True)
                
                records_processed = len(fact_internet_sales)
                utils_etl.log_etl_run(target_conn, 'Internet_Sales', 'Exitoso', records_processed)
                print(f"✓ Ventas por internet cargadas: {records_processed} registros")
            else:
                print("✗ Validación fallida para ventas por internet")
                
        except Exception as e:
            print(f"✗ Error procesando ventas por internet: {e}")
            utils_etl.log_etl_run(target_conn, 'Internet_Sales', 'Fallido')
            return
        
        # CARGAR HECHOS - VENTAS POR REVENDEDORES
        print("\n--- CARGANDO HECHOS: VENTAS POR REVENDEDORES ---")
        try:
            # Extraer y transformar ventas por revendedores
            reseller_sales = extract.extract_reseller_sales(
                source_conn, 
                start_date=etl_settings.get('start_date', '2011-01-01')
            )
            fact_reseller_sales = transform.transform_reseller_sales(reseller_sales, dimensions)
            
            # Validar transformación
            if transform.validate_transformations(fact_reseller_sales, 'fact_reseller_sales'):
                # Cargar datos
                if etl_settings.get('incremental_load', True):
                    load.load_incremental_fact_reseller_sales(fact_reseller_sales, target_conn)
                else:
                    load.load(fact_reseller_sales, target_conn, 'fact_reseller_sales', replace=True)
                
                records_processed = len(fact_reseller_sales)
                utils_etl.log_etl_run(target_conn, 'Reseller_Sales', 'Exitoso', records_processed)
                print(f"✓ Ventas por revendedores cargadas: {records_processed} registros")
            else:
                print("✗ Validación fallida para ventas por revendedores")
                
        except Exception as e:
            print(f"✗ Error procesando ventas por revendedores: {e}")
            utils_etl.log_etl_run(target_conn, 'Reseller_Sales', 'Fallido')
            return
        
        # CARGAR DATOS ADICIONALES - RAZONES DE VENTA
        print("\n--- CARGANDO DATOS ADICIONALES ---")
        try:
            sales_reason = extract.extract_sales_reason(source_conn)
            sales_reason_transformed = transform.transform_sales_reason(sales_reason)
            load.load(sales_reason_transformed, target_conn, 'dim_sales_reason', replace=False)
            print(f"✓ Razones de venta cargadas: {len(sales_reason_transformed)} registros")
        except Exception as e:
            print(f"Advertencia: Error cargando razones de venta: {e}")
        
        # MOSTRAR ESTADO FINAL
        print("\n--- PROCESO ETL COMPLETADO ---")
        status_after = utils_etl.get_etl_status(target_conn)
        print("Estado final del ETL:")
        for table, count in status_after.items():
            print(f"  {table}: {count} registros")
        
        # Registrar ejecución exitosa
        total_records = sum([count for count in status_after.values() if isinstance(count, int)])
        utils_etl.log_etl_run(target_conn, 'ETL_Completo', 'Exitoso', total_records)
        print(f"\n ETL completado exitosamente - Total registros: {total_records}")
        
    else:
        print("No hay datos nuevos para procesar")
        utils_etl.log_etl_run(target_conn, 'ETL_Completo', 'Sin_nuevos_datos')

if __name__ == "__main__":
    main()