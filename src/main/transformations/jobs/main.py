import datetime
import shutil

from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DateType

from resources.dev import *
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from resources.dev.config import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

############### Get S3 client ##############
aws_access_key = config.aws_access_key
#aws_access_key = ""
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

#Now you can use s3_client for your S3 operations
response = s3_client.list_buckets()
print(response)
logger.info("List of buckets: %s", response["Buckets"])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    statement = f"""
    select distinct file_name from
    {config.database_name}.{config.product_staging_table}
    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'I'
    """

    logger.info(f"dynamically statement created : {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run failed please check")
    else:
        logger.info("No record match")

else:
    logger.info("Last run was successfull")


try:
    s3_reader = S3Reader()
    #Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path = folder_path)
    logger.info( "Absolute path on s3 bucket for csv file %s" ,s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process}")

except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e

#just for reference when building
#Absolute path on s3 bucket for csv file ['s3://amzn-s3-pyspark/sales_data/calls.csv', 's3://amzn-s3-pyspark/sales_data/email_activity.csv', 's3://amzn-s3-pyspark/sales_data/sample_data.json']

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name is %s", bucket_name, local_directory)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder anme is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client,bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

#get a list of all files in the local diirectory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at any local directory after download {all_files}")

#filter files with .csv in their names and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request.")
        raise Exception("No csv data available to process the request.")

else:
    logger.error("There is no data to process.")
    raise Exception("There is no data to process the request.")

###########make csv lines convert into a list of comma separated###########
# csv_files = str(csv_files)[1:-1]
logger.info("****************Listing the file***************")
logger.info("List of csv files that needs to be processed %s", csv_files)

logger.info("****************Creating spark session*************")

spark = spark_session()

logger.info("**************** spark session created *************")

#check the required columns in the schema of csv files
#if not required column keep in a list in error files
#elswe union all the data into 1 dataframe

logger.info("***************checking schema for data loaded in s3******************")


correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", True)\
        .load(data).columns
    logger.info(f"Schema for {data} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns found for {data}")
        correct_files.append(data)

logger.info(f"************** List of correct files **************{correct_files}")
logger.info(f"************** List of error files ************** {error_files}")
logger.info(f"************** Moving Error data to error directory if any **************")

#move the data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved {file_name} from s3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix,  destination_prefix, file_name)
            logger.info(f"{message}")

        else:
            logger.error(f"'{file_path}' does not exist.")
else:
    logger.info("**************** There si no error files available in our dataset ****************")

#additional columns to needs to be taken care of
#determine extra column

#before running the process
#stage table needs to be updated with status as Active (A) or Inactive (I)
logger.info(f"**************** Updating the product_staging_table that we have started the process ****************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements= f"""INSERT INTO {db_name}.{config.product_staging_table}
        (file_name, file_location, created_date, status)
        VALUES ('{filename}', '{filename}', '{formatted_date}', 'A')
        """

        insert_statements.append(statements)
    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info("***************Connecting with mysql server ************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******************* mysql server connected successfully ****************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("*************** There is no files to process ************")
    raise Exception("******************* No data available with correct files ************")

logger.info("************** Staging table updated successfully **************")

logger.info("*************** Fixing extra column coming from source **************")

schema  = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", IntegerType(), True),
    StructField("additional_column", IntegerType(), True)
])

final_df_to_process = spark.createDataFrame([], schema= schema)
logger.info("*************** creating empty dataframe **************")
final_df_to_process.show()

for data in correct_files:
    data_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))

    logger.info(f"Extra columns present at source is {extra_columns}")

    if extra_columns:
        data_df = data_df.withColumn("additional_column",
                                     concat_ws(",", *extra_columns))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")

        logger.info(f"processed {data} and added 'additional_column'")

    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")


    final_df_to_process = final_df_to_process.union(data_df)
logger.info("*************** Final dataframe from source which will be going to processing *************")
final_df_to_process.show()

#enrisch the table from all dimension table
#also create a data mart for sales team and their incentive , address and all
#another datamart for cystomer who bought how much each day of month
#for every  month there should be a file  and   inside that
#there should be  a store id  seggregation
#read data from parquet and generate a csv file
#in which there will be a sales_person_name , sales_person_store-id
#sales_person_total_billing done for each month, total_incentive

#connecting with database reader
database_client = DatabaseReader(config.url, config.properties)
#creating df for all tables
#customer table
logger.info("************** Loading customer data into customer_table_df *****************")
customer_table_df = database_client.create_dataframe(
    spark,
    config.database_name,
    config.customer_table_name
)
#product table
logger.info("*************** Loading product table into product_table_df ***************")
product_table_df = database_client.create_dataframe(
    spark,
    config.database_name,
    config.product_table)

#product_staging_table
logger.info("****************Loading staaging table into product_staging_table ***************")
product_staging_table_df = database_client.create_dataframe(
    spark,
    config.database_name,
    config.product_staging_table)

#sales_team table
logger.info("************** loading sales team data into sales team table ***************")
sales_team_table_df = database_client.create_dataframe(
    spark,
    config.database_name,
    config.sales_team_table)

#store table
logger.info("***********loading store table into store-tabledf *********")
store_table_df = database_client.create_dataframe(
    spark,
    config.database_name,
    config.store_table)

s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)

#final enriched data
logger.info("*************** final enriched data ***************")
s3_customer_store_sales_df_join.show()


#write customer data into customer data mart in parquest format
#file will be written to local first
#move raw data to s3 bucket for reporting tolls
#write reporting data to mysql table also

logger.info("*************** write data into cust data mat **************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
            .select("ct.customer_id",
                    "ct.first_name", "ct.last_name","ct.address",
                    "ct.pincode", "phone_number",
                    "sales_date", "total_cost")
logger.info("*************** final data for cust data mart ***************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,
                                config.customer_data_mart_local_file)

logger.info(f"*************** customer data writen to local disk at  {config.customer_data_mart_local_file} ***************")

#move data to s3 bucket for cust data mart
logger.info(f"*************** data movement from local to s3  for cust data mart ***************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales team data mart
logger.info("*************** write data into sales team data mart ***************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
            .select("store_id",
                    "sales_person_id", "sales_person_first_name", "sales_person_last_name",
                    "store_manager_name", "manager_id", "is_manager",
                    "sales_person_address", "sales_person_pincode",
                    "sales_date", "total_cost",
                    expr("SUBSTR(sales_date,1,7) as sales_month"))

logger.info("*************** final data for sales team data mart ***************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                config.sales_team_data_mart_local_file)

logger.info(f"*************** sales team data written to local disk at {config.sales_team_data_mart_local_file} ***************")

#move data to s3 bucket for sales data mart
logger.info(f"*************** data movement from local to s3  for sales team data mart ***************")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#also writing data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
            .option("header", "true")\
            .mode("overwrite")\
            .partitionBy("sales_month", "store_id")\
            .option("path", config.sales_team_data_mart_partitioned_local_file)\
            .save()

#move data on s3 for partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path,
                                             config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

#calculation for cust mart
#find out customer total purchase every month
#write data to mysql table
logger.info("***************** calculating customer every month purchased amount ****************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("**************** calculation of customer mart done and written into the table ****************")

#calc for sales team mart
#find out the total sales done  by each sales person every month
#give the top performer, 1% incentive of total sales of month
#rest sales person gets nothing
#write data into mysql table

logger.info("************** calc sales every month billed amount ************")

sales_mart_calculation_table_write(final_sales_team_data_mart_df)

logger.info("************** cal of sales mart done and written into  the tbl **********")


############################## last step #################################
#move the file on  s3 into processed folder and delete the local files
source_prefix =config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("************** deleting  sales data from local ************")
delete_local_file(config.local_directory)
logger.info("************** deleted sales data from local************")

logger.info("*********** deleting customer_data_mart_local_file data from local ***************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("************* deleted customer_data_mart_local_file data from local ***************")

logger.info("************* deleting sales_team_data_mart_local_file data from local ***************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("************* deleted sales_team_data_mart_local_file data from local ****************")

logger.info("************* deleting sales_team_data_mart_partitioned_local_file data from local ***************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("************* deleted sales_team_data_mart_partitioned_local_file data from local ****************")

#update satus of staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements= f"UPDATE  {db_name}.{config.product_staging_table} " \
                    f"SET STATUS = 'I', updated_date= '{formatted_date}' " \
                    f"WHERE file_name = '{filename}'"

        update_statements.append(statements)
    logger.info(f"Updated statement created for satging table ------- {update_statements}")
    logger.info("************ Connecting with myssql server ****************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("*************** mysql server connected successfully **************")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("***************** there is some error in process in between ***************")
    sys.exit()


input("Press enter to terminate")