import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='src//main//pyscript//util/logging_to_file.conf')

# Get the custom Logger from Configuration File
logger = logging.getLogger(__name__)  # here we have usedd __name__ as the logger key name and this file name is same so __name__ will give same name


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info("load_files() is Started ...")

        if file_format == 'parquet':
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)

        elif file_format == 'csv':
            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)

    except Exception as exp:
        logger.error("Error in the method - load_files(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info(
            f"The input File {file_dir} is loaded to the data frame. The load_files() Function is completed !!! \n\n")
    return df
