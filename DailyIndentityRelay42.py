import os, shutil, gzip
import AppConfig as app

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import StringType
from azure.storage.blob import ContainerClient

sDate = date.today() - timedelta(days = 1)
today = str(sDate).replace("-", "")
account_name = app.setLakeAzure.account_name
container_name = app.setLakeAzure.container_name
sas_token = app.setLakeAzure.sas_token
sas_blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"
folder_prefix = app.setLakeAzure.folder_prefix
IndentitySourceFolder = app.AppPath.IndentitySourceFolder
SourceDir = f"{IndentitySourceFolder}/{today}/"
ExportDir = f"{app.AppPath.ExportSourceFolder}/IdentityRelay42/"
blobFile = ""

class ImportIndentityRelay42:    
    def ImportID():
        try:
            blob_service_client = ContainerClient.from_container_url(sas_blob_url)
            blob_list = blob_service_client.walk_blobs(name_starts_with=f"{folder_prefix}/")

            for blob in blob_list:
                if blob.name[blob.name.index('/')+1:blob.name.index('/')+9] == today :
                    blobFile = blob.name
                    
            if not os.path.exists(f"{IndentitySourceFolder}/{today}/"):
                os.makedirs(f"{IndentitySourceFolder}/{today}/")

            download_file_path = f"{IndentitySourceFolder}/{today}/{blobFile[23:]}"
            extracted_file_path = f"{IndentitySourceFolder}/{today}/{blobFile[23:len(blobFile)-3]}"
            blob_client = blob_service_client.get_blob_client(blobFile)

            with open(download_file_path, "wb") as my_blob:
                blob_data = blob_client.download_blob()
                my_blob.write(blob_data.readall())

            with gzip.open(download_file_path, "rb") as gzipped_file, open(extracted_file_path, "wb") as extracted_file:
                extracted_data = gzipped_file.read()
                extracted_file.write(extracted_data)

            if os.path.exists(download_file_path):
                os.remove(download_file_path)

        except Exception as e:
            app.logging.error(f"An error occurred: {e}")
            return False
        else:
            app.logging.info(f"Upload Sucess")
            return True
        finally:
            app.logging.info(f"End Finish")

class ExportIndentityRelay42:
    def ExportID():
        os.environ["HADOOP_HOME"] = app.AppPath.hadoop
        spark = SparkSession.builder \
            .appName("Daily Transaction ID to Parquet") \
            .config("spark.driver.extraClassPath", app.setProperties.configs.get("spark_driver").data) \
            .config("spark.local.dir", app.setProperties.configs.get("spark_log_dir").data) \
            .config("spark.driver.memory", app.setProperties.configs.get("spark_memory").data) \
            .config("spark.executor.memory", app.setProperties.configs.get("spark_executor").data) \
            .getOrCreate()
        try:
            if not os.path.exists(ExportDir):
                os.makedirs(ExportDir)

            for IndentityFile in os.listdir(SourceDir):
                IndentityFile = os.path.join(SourceDir, IndentityFile)
                json_df = spark.read.json(IndentityFile)
                json_df = json_df.withColumn("trackId", json_df.trackId.cast(StringType()))
                json_df = json_df.withColumn("partnerNumber", json_df.partnerNumber.cast(StringType()))
                json_df = json_df.withColumn("partnerCookie", json_df.partnerCookie.cast(StringType()))
                json_df_filtered = json_df.filter(json_df.partnerNumber != 2010)
                json_df_filtered = json_df_filtered.withColumn("partnerNumber", when(json_df_filtered.partnerNumber == 4242, 42).otherwise(json_df_filtered.partnerNumber))
                result_df = json_df_filtered.selectExpr("trackId as UUID", "partnerNumber as key", "partnerCookie as Value")
                result_df.write.parquet(f"{ExportDir}\\partition_date={sDate}\\")

        except Exception as e:
            app.logging.error(f"Generate to Parquet error occurred: {e}")
            return False
        else:
            app.logging.info(f"Generate Indentity Relay42 Sucess")
            return True
        finally:
            app.logging.info(f"Generate Indentity Relay42 Finish")
            spark.stop()

class DeleteRawFile:     
    def DeleteRawFile():
        for filename in os.listdir(SourceDir):
            file_path = os.path.join(SourceDir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    shutil.rmtree(SourceDir)
                    shutil.rmtree(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                app.logging.error(f"Failed to delete: {file_path, e}")
                return False
            else:
                app.logging.info(f"Delete Indentity Relay42 Sucess")
                return True