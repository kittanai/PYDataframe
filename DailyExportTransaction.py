import os, shutil, gzip
import AppConfig as app
import pandas as pd
import pyodbc

from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from azure.storage.blob import BlobServiceClient

server = app.setDatabase.server
database = app.setDatabase.database
username = app.setDatabase.username
password = app.setDatabase.password
driver = '{ODBC Driver 17 for SQL Server}'
jdbc_url = f"jdbc:sqlserver://{server};database={database};user={username};password={password}"

os.environ["HADOOP_HOME"] = app.AppPath.hadoop

class ExportDailyTransaction:    
    def ExportToParquetFile():        
        spark = SparkSession.builder \
            .appName("Daily Export Transaction to Parquet Files") \
            .config("spark.driver.extraClassPath", app.setProperties.configs.get("spark_driver").data) \
            .config("spark.local.dir", app.setProperties.configs.get("spark_log_dir").data) \
            .config("spark.driver.memory", app.setProperties.configs.get("spark_memory").data) \
            .config("spark.executor.memory", app.setProperties.configs.get("spark_executor").data) \
            .getOrCreate()
        sourceFolder = app.AppPath.ExportSourceFolder
        sDate = date.today() - timedelta(days = 2)
        lists = ["product_info", "conversion", "event_performance", "page_view"]
        errorCount = 0
        for dbTable in lists:
            output_dir = f"{sourceFolder}/{str(dbTable).replace('_', '')}/"
            query = f"(SELECT *, CONVERT(DATE, [timestamp]) as partition_date FROM {dbTable} WHERE CONVERT(DATE, [timestamp]) = ('{sDate}')) AS data"
            try:
                df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                    .option("dbtable", query) \
                    .load()
                if df.count() <= 0:
                    app.logging.error(f"No Record on Table: {dbTable}")
                else:
                    df = df.withColumn("id", df.id.cast(StringType()))
                    df = df.withColumn("ref_id", df.ref_id.cast(StringType()))
                    df = df.withColumn("track_id", df.track_id.cast(StringType()))
                    df = df.withColumn("timestamp", df.timestamp.cast("timestamp"))
                    df = df.withColumn("data_date", df.data_date.cast("timestamp"))
                    df = df.withColumn("effective_date", df.effective_date.cast("timestamp"))
                    df = df.withColumn("session_id", df.session_id.cast(StringType()))
                    if str(dbTable).replace('_', '') =="productinfo":
                        df = df.withColumn("event_id", df.event_id.cast(StringType()))
                        df = df.withColumn("interaction_type", df.interaction_type.cast(StringType()))
                        df = df.withColumn("site_number", df.site_number.cast(StringType()))
                        df = df.withColumn("type", df.type.cast(StringType()))
                        df = df.withColumn("r42_product_brand", df.r42_product_brand.cast(StringType()))
                        df = df.withColumn("r42_product_name", df.r42_product_name.cast(StringType()))
                        df = df.withColumn("r42_price_option", df.r42_price_option.cast(StringType()))
                        df = df.withColumn("user_agent", df.user_agent.cast(StringType()))
                        df = df.withColumn("url", df.url.cast(StringType()))
                        df = df.withColumn("r42_price", df.r42_price.cast(StringType()))
                        df = df.withColumn("r42_product_size", df.r42_product_size.cast(StringType()))
                        df = df.withColumn("r42_product_id", df.r42_product_id.cast(StringType()))
                        df = df.withColumn("r42_product_colour", df.r42_product_colour.cast(StringType()))
                    elif str(dbTable).replace('_', '') == "pageview":
                        df = df.withColumn("event_id", df.event_id.cast(StringType()))
                        df = df.withColumn("interaction_type", df.interaction_type.cast(StringType()))
                        df = df.withColumn("site_number", df.site_number.cast(StringType()))
                        df = df.withColumn("type", df.type.cast(StringType()))
                        df = df.withColumn("keywords", df.keywords.cast(StringType()))
                        df = df.withColumn("cx_id", df.cx_id.cast(StringType()))
                        df = df.withColumn("url", df.url.cast(StringType()))
                        df = df.withColumn("site", df.site.cast(StringType()))
                        df = df.withColumn("category", df.category.cast(StringType()))
                        df = df.withColumn("content",df.content.cast(StringType()))
                        df = df.withColumn("sub_category1", df.sub_category1.cast(StringType()))
                        df = df.withColumn("sub_category2", df.sub_category2.cast(StringType()))
                        df = df.withColumn("language", df.language.cast(StringType()))
                        df = df.withColumn("channel", df.channel.cast(StringType()))
                        df = df.withColumn("cart_product_lists", df.cart_product_lists.cast(StringType()))
                        df = df.withColumn("private_id2", df.private_id2.cast(StringType()))
                        df = df.withColumn("utm_source", df.utm_source.cast(StringType()))
                        df = df.withColumn("utm_medium", df.utm_medium.cast(StringType()))
                        df = df.withColumn("utm_term", df.utm_term.cast(StringType()))
                        df = df.withColumn("referrer_url", df.referrer_url.cast(StringType()))
                        df = df.withColumn("name", df.name.cast(StringType()))
                        df = df.withColumn("utm_campaign", df.utm_campaign.cast(StringType()))
                    elif str(dbTable).replace('_', '') == "conversion":
                        df = df.withColumn("event_id", df.event_id.cast(StringType()))
                        df = df.withColumn("interaction_type", df.interaction_type.cast(StringType()))
                        df = df.withColumn("site_number", df.site_number.cast(StringType()))
                        df = df.withColumn("type", df.type.cast(StringType()))
                        df = df.withColumn("r42_product_size", df.r42_product_size.cast(StringType()))
                        df = df.withColumn("r42_product_price", df.r42_product_price.cast(StringType()))
                        df = df.withColumn("r42_product_status", df.r42_product_status.cast(StringType()))
                        df = df.withColumn("r42_product_name", df.r42_product_name.cast(StringType()))
                        df = df.withColumn("r42_product_duration", df.r42_product_duration.cast(StringType()))
                        df = df.withColumn("r42_product_id", df.r42_product_id.cast(StringType()))
                        df = df.withColumn("r42_product_brand", df.r42_product_brand.cast(StringType()))
                        df = df.withColumn("r42_product_lists", df.r42_product_lists.cast(StringType()))
                        df = df.withColumn("r42_payment_status", df.r42_payment_status.cast(StringType()))
                        df = df.withColumn("r42_page_type", df.r42_page_type.cast(StringType()))
                        df = df.withColumn("r42_total_price", df.r42_total_price.cast(StringType()))
                        df = df.withColumn("r42_order_id", df.r42_order_id.cast(StringType()))
                        df = df.withColumn("r42_payment_method_type", df.r42_payment_method_type.cast(StringType()))
                        df = df.withColumn("r42_payment_method_brand", df.r42_payment_method_brand.cast(StringType()))
                    elif str(dbTable).replace('_', '') == "eventperformance":
                        df = df.withColumn("site_id", df.site_id.cast(StringType()))
                        df = df.withColumn("engagement_type", df.engagement_type.cast(StringType()))
                        df = df.withColumn("event_type", df.event_type.cast(StringType()))
                        df = df.withColumn("ad_id", df.ad_id.cast(StringType()))
                        df = df.withColumn("line_item_id", df.line_item_id.cast(StringType()))
                        df = df.withColumn("au_id", df.au_id.cast(StringType()))
                        df = df.withColumn("creative_id", df.creative_id.cast(StringType()))
                        df = df.withColumn("content_name", df.content_name.cast(StringType()))
                        df = df.withColumn("content_ids", df.content_ids.cast(StringType()))
                        df = df.withColumn("content_category", df.content_category.cast(StringType()))
                        df = df.withColumn("partner_name", df.partner_name.cast(StringType()))
                        df = df.withColumn("content_subcategory2", df.content_subcategory2.cast(StringType()))
                        df = df.withColumn("action", df.action.cast(StringType()))
                        df = df.withColumn("currency", df.currency.cast(StringType()))
                        df = df.withColumn("content_subcategory", df.content_subcategory.cast(StringType()))
                        df = df.withColumn("order_id", df.order_id.cast(StringType()))
                        df = df.withColumn("value", df.value.cast(StringType()))
                        df = df.withColumn("bucket_size", df.bucket_size.cast(StringType()))
                    else:
                        app.logging.error('No Table for Read')

                    df.write.parquet(output_dir, mode="append", partitionBy="partition_date")
                    df.unpersist()
            except Exception as e:
                app.logging.error(f"An error occurred: {e}")
                errorCount = errorCount + 1
            else:
                app.logging.info(f"Import Table:{dbTable} Sucess")
            finally:
                app.logging.info(f"Table:{dbTable} Finish")
        spark.stop()

        if errorCount<=0:
            return True
        else:
            return False

    def ManualExportToParquetFileByTable(self, sTable, sDate):        
        print(self,sTable)
        print(self,sDate)

    def ExportToCSV():
        spark = SparkSession.builder \
            .appName("Daily Export CSV to Parquet Files") \
            .config("spark.driver.extraClassPath", app.setProperties.configs.get("spark_driver").data) \
            .config("spark.local.dir", app.setProperties.configs.get("spark_log_dir").data) \
            .config("spark.driver.memory", app.setProperties.configs.get("spark_memory").data) \
            .config("spark.executor.memory", app.setProperties.configs.get("spark_executor").data) \
            .getOrCreate()
        
        sDate = date.today()
        newDate = str(sDate).replace("-", "")
        dbTable = 'page_view'
        dbFolder = dbTable.replace("_", "" )
        f_string = dbFolder[0:1].upper()
        dbFile = f_string+dbFolder[1:len(dbFolder)]
        output_dir = f"{app.AppPath.ExportCSVSourceFolder}\{dbFile}{newDate}"

        query = f"(SELECT track_id, site, category, utm_source, utm_medium, "
        query = f"{query}utm_term, utm_campaign, Convert(date,timestamp) as Datetimestamp, "
        query = f"{query}data_date, COUNT(track_id) as Trackidcount "
        query = f"{query}FROM dbo.page_view WHERE data_date = '{sDate}' AND site is not null " 
        query = f"{query}group by track_id ,site ,category ,utm_source, "
        query = f"{query}utm_medium ,utm_term ,utm_campaign ,Convert(date,timestamp) ,data_date) AS data "

        try:
            df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable", query) \
            .load()
            if df.count() <= 0:
                app.logging.error(f"No Record")
            else:
                df.write.csv(output_dir, mode="overwrite")
        except Exception as e:
            app.logging.error(f"An error occurred: {e}")
            return False
        else:
            app.logging.info(f"Import Daily CSV Sucess")
            return True
        finally:
            app.logging.info(f"Daily CSV Finish")
            spark.stop()
        
    def ManualExport():
        print("xxx")

class ClearDailyTransaction:
    def UploadTOBlob():
        account_name = app.setRelayAzure.account_name
        container_name = app.setRelayAzure.container_name
        sas_token = app.setRelayAzure.sas_token
        sourceFolder = app.AppPath.ExportSourceFolder
        try:
            blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)
            container_client = blob_service_client.get_container_client(container_name)
            for root, dirs, files in os.walk(sourceFolder):
                for fileName in files:
                    local_file_path = os.path.join(root, fileName)
                    blob_name = local_file_path.replace(sourceFolder, "").replace("\\", "/")
                    blob_name = f"/{blob_name}"
                    blob_client = container_client.get_blob_client(blob_name)
                    with open(local_file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                    os.remove(local_file_path)
            shutil.rmtree(sourceFolder)
        except Exception as e:
            app.logging.error(f"An error occurred: {e}")
            return False
        else:
            app.logging.info(f"Upload Sucess")
            return True
        finally:
            app.logging.info(f"Upload Finish")

    def ClearTransactionData():
        currentTimeDate = datetime.now() - relativedelta(months=1)
        sDate = currentTimeDate.strftime('%Y-%m-%d')
        try:
            conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}")
            lists = ["conversion", "product_info", "event_performance", "page_view"]
            for dbTable in lists:
                query = f"DELETE [dbo].[{dbTable}] WHERE CONVERT(DATE, [timestamp]) = '{sDate}'"
                cursor = conn.cursor()
                cursor.execute(query)
            conn.close
        except Exception as e:
            app.logging.error(f"An error occurred: {e}")
            return False
        else:
            app.logging.info(f"Clear Sucess")
            return True
        finally:
            app.logging.info(f"Clear Finish")