import DailyExportTransaction as dailyExport
import AppConfig as app

'''checkPoint1 = dailyExport.ExportDailyTransaction.ExportToParquetFile()
if checkPoint1 == True:
    checkPoint2 = dailyExport.ExportDailyTransaction.ExportToCSV()
else:
    app.logging.error(f"Export Parquet Error")'''
checkPoint2 = dailyExport.ExportDailyTransaction.ExportToCSV()