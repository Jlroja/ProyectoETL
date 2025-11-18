# ETL – Datamart de Internet Sales y Reseller Sales (AdventureWorks2022)

### Desarrollado por 

### ANGIE MELISSA OCORO HURTADO 2310176-3743
### JUAN CAMILO LOPEZ QUINTANA 2310177-3743
### VICTOR DANIEL ACUÑA SALAZAR 2310114-3743
### JAVIER ANDRES LASSO ROJAS 2061149-3743
### SEBASTIAN BOLAÑOS MORALES 2310168-3743


Este proyecto implementa un proceso **ETL completo en Python** para construir dos datamarts:

- **Internet Sales**
- **Reseller Sales**

a partir de la base de datos operacional **AdventureWorks2022** (SQL Server).  
Los datos transformados se cargan en una bodega en **PostgreSQL**.

---

## 📌 Características principales

 Extracción desde SQL Server con SQLAlchemy + pyodbc  
 Transformación con pandas (limpieza, normalización, surrogate keys, joins)  
 Carga final en PostgreSQL  
 Construcción de las siguientes dimensiones:

- `dim_date`
- `dim_product`
- `dim_customer`
- `dim_territory`
- `dim_reseller`
- `dim_salesperson`

 Construcción de los hechos:

- `fact_internet_sales`
- `fact_reseller_sales`

 Notebooks para validar cada etapa del ETL  
 Configuración externa vía archivo `config_fill.yml`

---


---

## 🛠 Instalación del ambiente

### 1. Crear entorno virtual

**Linux / Mac**
```bash
python3 -m venv my_env
source my_env/bin/activate


## 🛠 Instalación de dependencias 

pip install -r requirements.txt

### Si flata algun driver

pip install psycopg2
pip install psycopg2-binary


### Configuracion del archivo config_fill.yml

Adventure_Works:
  drivername: mssql+pyodbc   
  host: localhost
  dbname: AdventureWorks2022
  trusted_connection: yes
  driver: ODBC Driver 17 for SQL Server

ETL_PRO:
  drivername: postgresql
  user: postgres
  password: tu_password
  host: localhost
  port: 5432
  dbname: adventureworks_DW



