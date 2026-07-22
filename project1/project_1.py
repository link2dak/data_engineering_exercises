from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, DateType, BooleanType


# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("DataIngestion")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://link2-bucket-rev-005311909391-us-east-2-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

orders_schema = StructType([
    StructField('order_id', StringType()),
    StructField('customer_id', IntegerType()),
    StructField('product_id', StringType()),
    StructField('order_date', DateType()), 
    StructField('ship_date', DateType()), 
    StructField('quantity', IntegerType()),
    StructField('unit_price', FloatType()),
    StructField('discount_pct', FloatType()),
    StructField('total_amount', FloatType()),
    StructField('payment_method', StringType()),
    StructField('order_status', StringType())
])
product_schema = StructType([
    StructField('product_id', StringType()),
    StructField('product_name', StringType()),
    StructField('category', StringType()),
    StructField('brand', StringType()), 
    StructField('price', FloatType()), 
    StructField('cost', FloatType()),
    StructField('stock_quantity', IntegerType()),
    StructField('weight_kg', FloatType()),
    StructField('created_date', DateType()),
    StructField('is_active', BooleanType()),
])
customer_schema = StructType([
    StructField('customer_id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()), 
    StructField('phone', StringType()), 
    StructField('signup_date', DateType()),
    StructField('country', StringType()),
    StructField('state', StringType()),
    StructField('postal_code', StringType()),
    StructField('is_active', BooleanType()),
    StructField('loyalty_points', IntegerType()),
])

# Read the Titanic dataset from a Parquet file stored in S3
# and load it into a Spark DataFrame.
orders_df = spark.read.csv("s3://link2-bucket-rev-005311909391-us-east-2-an/orders.csv", header=True, schema=orders_schema)
products_df = spark.read.csv("s3://link2-bucket-rev-005311909391-us-east-2-an/products.csv", header=True, schema = product_schema)
customer_df = spark.read.csv("s3://link2-bucket-rev-005311909391-us-east-2-an/customers.csv", header=True, schema=customer_schema)

# orders_df = spark.read.csv(
#     "orders.csv", header=True, schema=orders_schema
# )

# products_df = spark.read.csv("products.csv", header=True, schema = product_schema)
# customer_df = spark.read.csv("customers.csv", header=True, schema=customer_schema)



# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
orders_df.printSchema()
products_df.printSchema()
customer_df.printSchema()

# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.

# TODO: uncomment this
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

#===============cleaning data for customers file============================

customers_df_clean = customer_df

target_cols = ['first_name', 'last_name', 'email', 'phone', 'country', 'state', 'postal_code']
for c in target_cols:
    customers_df_clean = customers_df_clean.withColumn(c, F.trim(F.col(c)))


#  Regex pattern for a standard email
email_pattern = r"^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$"
# Keep only strings that match the email pattern, otherwise return null
customers_df_clean = customers_df_clean.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.col("email")
    )
)
phone_pattern = r"^(\+?\d{1,3}[\s.-]?)?(\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}$"
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.when(
        F.col("phone").rlike(phone_pattern),
        # changing the number so that there are dashes in between them
        F.regexp_replace(F.col("phone"), r"(\d{3})(\d{3})(\d{4})", r"$1-$2-$3")
    )
) 


customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    F.when(F.col("loyalty_points") < 0, 0)
     .otherwise(F.col("loyalty_points"))
)
customers_df_clean = customers_df_clean.replace({"U.S.A.":"USA"}, subset=['country'])
# # filter out all the unreasonably large loyalty_values
# customers_df_clean = customers_df_clean.filter(F.col('loyalty_points') <= 1000)

# clean the state
customers_df_clean = customers_df_clean.withColumn('state', F.upper('state'))

customers_df_clean = customers_df_clean.dropDuplicates().dropna()

# customers_df_clean.show()


#============== cleaning data for products table ============
cleaned_products_df = products_df


# getting rid of all the bad Ids by returning NULL if the id doesnt match pattern
id_pattern = r"P\d{4}$"
cleaned_products_df = cleaned_products_df.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(id_pattern),
        F.col("product_id")
    )
)

# cleaning the product names
cleaned_products_df = cleaned_products_df.withColumn(
    "product_name", F.regexp_replace('product_name', '"+$', "")
)

# cleaning the brand names so that only the initial letter is capital
cleaned_products_df = cleaned_products_df.withColumn(
    "brand", F.upper('brand')
)

# replace the values with good ones
cleaned_products_df = cleaned_products_df.withColumn('price', F.abs('price'))

# make sure the stock quantitity is of good value
cleaned_products_df = cleaned_products_df.withColumn('stock_quantity', 
                                                    F.when(F.col('stock_quantity') > 0, F.col('stock_quantity')) )

cleaned_products_df = cleaned_products_df.drop_duplicates().dropna()
# cleaned_products_df.show(23)


# ============ cleaning the data for the orders.csv ===========
cleaned_orders_df = orders_df

# get rid of all the negative numbers in the table
collumns = ['quantity','unit_price', 'discount_pct', 'total_amount']
for c in collumns:
    cleaned_orders_df = cleaned_orders_df.withColumn(c, F.abs(c))

#getting rid of all the bad product id patterns
id_pattern = r"P\d{4}$"
cleaned_orders_df = cleaned_orders_df.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(id_pattern),
        F.col("product_id")
    )
)

# getting all the orders with valid product IDs
cleaned_orders_df = (
    cleaned_orders_df.join(
        cleaned_products_df.select("product_id").distinct(),
        on="product_id",
        how="left_semi"
    )
)

# getting all the orders with valid customer IDs
cleaned_orders_df = (
    cleaned_orders_df.join(
        customers_df_clean.select("customer_id").distinct(),
        on="customer_id",
        how="left_semi"
    )
)


cleaned_orders_df = cleaned_orders_df.drop_duplicates().dropna()
# check if the quantity and the discount add up to the total
cleaned_orders_df = cleaned_orders_df.withColumn('total_amount',
    F.round((F.col('quantity')*F.col('unit_price')) - F.col('discount_pct'), 2)
)
# cleaned_orders_df.show(25)



# Write the DataFrame as an Iceberg table.
(
    customers_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

cleaned_orders_df.writeTo("glue_catalog.iceberg_catalog_db.orders").using("iceberg").createOrReplace()
cleaned_products_df.writeTo("glue_catalog.iceberg_catalog_db.products").using("iceberg").createOrReplace()




# Query the newly created Iceberg table to verify that the
# data was written successfully.


print("amount number of records")

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
""").show(23)

# query the data from the orders table
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.orders
""").show(23)

# query from the products tabel
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.products
""").show(23)





# Stop the Spark session and release cluster resources.
spark.stop()