# Test 1 - PostgreSQL, S3, Iceberg

Assume that we have a sample PostgreSQL DB with three tables:

- Orders
- Customers
- Employees

[Northwind Create Script - northwind_db_create_script.sql](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/northwind_db_create_script.sql)

## Step 1: Glue Job - Load PostgreSQL tables into S3 bucket

### tt-load-from-postgresql

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_url = "jdbc:postgresql://XXX.rds.amazonaws.com/iceberg_test"
db_user = "postgres"
db_password = "Admin123"
s3_path = "s3://XXX/postgresql-source-files/"

orders_df = spark.read.format("jdbc").option("url",db_url).option("user", db_user).option("password", db_password).option("dbtable","orders").load()
customers_df = spark.read.format("jdbc").option("url",db_url).option("user", db_user).option("password", db_password).option("dbtable","customers").load()
employees_df = spark.read.format("jdbc").option("url",db_url).option("user", db_user).option("password", db_password).option("dbtable","employees").load()

orders_pd = orders_df.toPandas()
customers_pd = customers_df.toPandas()
employees_pd = employees_df.toPandas()

orders_pd.to_parquet(s3_path + "orders.parquet")
customers_pd.to_parquet(s3_path + "customers.parquet")
employees_pd.to_parquet(s3_path + "employees.parquet")

job.commit()
```

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/load_from_postgresql.png)

## Step 2: Create a new database in Glue Data Catalog

<kbd>Glue</kbd> > <kbd>Data Catalog</kbd> > <kbd>Databases</kbd> > <kbd>Create a database</kbd>

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/glue_create_database.png)

## Step 3: Create a crawler for the files loaded from PostgreSQL

<kbd>AWS Glue</kbd> > <kbd>Databases</kbd> > <kbd>test-iceberg-db</kbd> > <kbd>Add tables using crawler</kbd>

### Set crawler properties

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/crawler_1.png)

### Choose data sources and classifiers

<kbd>Data sources</kbd> > <kbd>Add data source</kbd>

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/crawler_2.png)

### Configure security settings

<kbd>IAM role</kbd> > <kbd>Existing IAM role</kbd> or \
<kbd>IAM role</kbd> > <kbd>Create a new role</kbd>

### Set output and scheduling

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/crawler_3.png)

### Review and create

<kbd>Create crawler</kbd>

## Step 4: Run crawler

<kbd>AWS Glue</kbd> > <kbd>Crawlers</kbd> > <kbd>crw-test-iceberg</kbd> > <kbd>Run crawler</kbd>

## Step 5: Athena - Create Iceberg tables
### ib_orders
```
CREATE TABLE test_iceberg_db.ib_orders (
  order_id int,
  customer_id string)
LOCATION 's3://tt-bucket-001/test-warehouse/orders' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
)
```
### ib_customers
```
CREATE TABLE test_iceberg_db.ib_customers (
  customer_id string,
  company_name string)
LOCATION 's3://tt-bucket-001/test-warehouse/customers' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
)
```
### ib_employees
```
CREATE TABLE test_iceberg_db.ib_employees (
  employee_id int,
  full_name string)
LOCATION 's3://tt-bucket-001/test-warehouse/employees' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
)
```

## Step 6: Check catalog database and tables

After ingested files into S3 bucket and crawled the loaded files, three new tables should appear in <kbd>test-iceberg-db</kbd> database.

<kbd>AWS Glue</kbd> > <kbd>Databases</kbd> > <kbd>test-iceberg-db</kbd>

![](https://github.com/talhaturan/iceberg/blob/95e733da9575afb69716905632922df922601582/crawler_4.png)

## Step 7: Glue Job - Create Iceberg Tables

### Job parameters

#### --datalake-formats
```
iceberg
```

#### --conf
```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions 
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog 
--conf spark.sql.catalog.glue_catalog.warehouse=s3://tt-bucket-001/test-warehouse/
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog 
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

[AWS Docs link](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html#aws-glue-programming-etl-format-iceberg-enable)

### tt-iceberg-test
```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Setting configuration in the conf object

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

warehouse_path = "s3://XXX/test-warehouse/"

pg_orders_df = glueContext.create_dynamic_frame.from_catalog(database="test_iceberg_db", table_name="postgre_orders").toDF()
pg_customers_df = glueContext.create_dynamic_frame.from_catalog(database="test_iceberg_db", table_name="postgre_customers").toDF()
pg_employees_df = glueContext.create_dynamic_frame.from_catalog(database="test_iceberg_db", table_name="postgre_employees").toDF()

pg_orders_df.createOrReplaceTempView("vw_pg_orders")
pg_customers_df.createOrReplaceTempView("vw_pg_customers")
pg_employees_df.createOrReplaceTempView("vw_pg_employees")

query = f"""
    insert into glue_catalog.test_iceberg_db.ib_orders
    select order_id, customer_id
    from vw_pg_orders;
"""
spark.sql(query)

query = f"""
    insert into glue_catalog.test_iceberg_db.ib_customers
    select customer_id, company_name
    from vw_pg_customers;
"""
spark.sql(query)

query = f"""
    insert into glue_catalog.test_iceberg_db.ib_employees
    select employee_id, concat(first_name, ' ', last_name) as full_name
    from vw_pg_employees;
"""
spark.sql(query)

job.commit()
```

## Step 7: Check Iceberg table file structure

![](https://github.com/talhaturan/iceberg/blob/fbf98a496a8b47fcb798db22c8be2c562b533a6d/file_structure.png)
