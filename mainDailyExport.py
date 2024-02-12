import DailyIndentityRelay42 as dailyImport
import AppConfig as app

checkPoint1 = dailyImport.ImportIndentityRelay42.ImportID()
if checkPoint1 == True:
    checkPoint2 = dailyImport.ExportIndentityRelay42.ExportID()
    if checkPoint2 == True:
        checkPoint3 = dailyImport.DeleteRawFile.DeleteRawFile()
    else:
        app.logging.error(f"Export ID Error")
else:
    app.logging.error(f"Import ID Error")