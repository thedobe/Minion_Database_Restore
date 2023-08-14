# Minion_Database_Restore
A wrapper for generating a database restore script

## Requirements
* Minion
* SQL Server 2019 RTM (or a string_split() equivalent)
* Utility database

## Functionality
### Parameters
* @DBName
  * the database to be stored
* @RestoreName
  * the restore name of the database of @DBName
* @DataFileName
  * the database file name
    * doesn't need to match the mdf (but can cause problems if it doesn't match)
* @LogFileName
  * the database file name
    * doesn't need to match the ldf (but can cause problems if it doesn't match)
* @DownloadPath (optional)
  * the download file path of any s3 objects
* @MoveLocation
  * the URI to the data/log files
    * if your log file is on a separate LUN you will need to address in the output as this script is for a shared LUN
* @StartDate
  * the start date of the restore (e.g., 2023-08-01)
* @EndDate
  * the end date of the restore (e.g., 2023-08-07)
* @StopAt
  * the point in time datetime which you want the logs to restore (e.g., 2023-08-07 12:00)

### Gist
* Build a list of full and diff backup file(s) with optional logs if a point in time is required
   * the default behavior is set to REPLACE
* Basic error checking
* Generic s3 download file(s) template

### Example Usage
* Restore myDatabase from a start date 
  * `EXEC [dbo].[minion_database_restore] 
	     @DBName = 'myDatabase',
       @RestoreName = 'myDatabase',
       @DataFileName = 'myDatabase_data',
       @LogFileName = 'myDatabase_log',
       @MoveLocation  = '\\some\path',
       @StartDate = '2023-08-01',
       @EndDate = '2023-08-07'
* Restore myDatabase from a start date and ending at a specific datetime
  * `EXEC [dbo].[minion_database_restore] 
	     @DBName = 'myDatabase',
       @RestoreName = 'myDatabase',
       @DataFileName = 'myDatabase_data',
       @LogFileName = 'myDatabase_log',
       @MoveLocation  = '\\some\path',
       @StartDate = '2023-08-01',
       @EndDate = '2023-08-07',
       @StopAt = '2023-08-07 12:00'`
 
### Note(s)
* The s3 functionality has been commented out in this version, but if needed uncomment and add a @DownloadPath
* The script is generated as an output and will not automate a restore
