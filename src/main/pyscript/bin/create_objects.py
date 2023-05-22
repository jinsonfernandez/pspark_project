from pyspark.sql import SparkSession
import logging
import logging.config

# Load the logging configuration for custom logger

# logging.config.fileConfig('src\\main\\pyscript\\utils\\logging_to_file.conf')
# logger = logging.getLogger("create_objects")


def get_spark_object(envn, appName):
    try:
        print(f"get_spark_object() has started. The {envn} env is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()

    except NameError as exp:
        print(f"There is name error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    except Exception as exp:
        print(f"There is an error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    else:
        print("Spark Object is Created.. ")
    return spark

