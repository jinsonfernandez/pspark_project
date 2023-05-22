import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import logging
import logging.config
import os
from presc_run_data_ingest import load_files

logging.config.fileConfig(fname='src\\main\\pyscript\\util\\logging_to_file.conf')


def main():
    try:
        logging.info("Main Method has Started..")
        # get all variables
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("Spark object is created")

        # get spark object
        # Validate the spark object
        get_curr_date(spark)

        # Set logging configuration mechanism
        # Set up error handling

        # Ingest the data
        for file in os.lisdir(gav.staging_dim_city):

            print(f"file is {file}")
            file_dir = f"{gav.staging_dim_city}\\{file}"
            print(file_dir)

            if file_dir.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

            elif file_dir.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        # Load the city file
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        # Validate city file
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')
        df_print_schema(df_city, 'df_city')

        # Load the fact file
        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        # validate the fact files
        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')
        df_print_schema(df_fact, 'df_fact')

        # Data preprocessing script

        # Data cleaning step
        # Data validation
        # Set up logging configuration mechanism
        # Set up error handling

        # Data Transformation step
        # Apply all transformation logic
        # Data validation
        # Set up logging configuration mechanism
        # Set up error handling

        # Data Extraction
        # Apply all transformation logic
        # Data validation
        # Set up logging configuration mechanism
        # Set up error handling
        logging.info("Pipeline has been completed")
    except Exception as e:
        logging.error(
            f"Exception Occured in the main method - {str(e)}.\nPlease check the Stack Trace to go to respective module and fix it.",
            exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info("run_presc has been started")
    main()
