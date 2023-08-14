CREATE OR ALTER  PROCEDURE [dbo].[database_restore] (
       @DBName SYSNAME,
       @RestoreName SYSNAME = NULL,
       @DataFileName SYSNAME = NULL,
       @LogFileName SYSNAME = NULL,
       @DownloadPath VARCHAR(2000) = NULL,
       @MoveLocation VARCHAR(255),
       @StartDate DATETIME = NULL,
       @EndDate DATETIME,
       @StopAt VARCHAR(50) = NULL

) AS
BEGIN

       SET NOCOUNT ON; 

       /*
       --     DEBUG
       DECLARE 
	   @DBName SYSNAME = '<database>',
       @RestoreName SYSNAME = '<restore_name>',
       @DataFileName SYSNAME = NULL,
       @LogFileName SYSNAME = NULL,
       @DownloadPath VARCHAR(2000) = NULL,
       @MoveLocation VARCHAR(255) = '<path>',
       @StartDate DATETIME = NULL,
       @EndDate DATETIME = '<datetime>',
       @StopAt VARCHAR(50) = '<datetime>'
       */      

       IF (@DataFileName IS NULL)
       BEGIN
             SET @DataFileName = '<< data file name >>'
       END 

       IF (@LogFileName IS NULL)
       BEGIN
             SET @LogFileName = '<< log file name >>'
       END 

       IF (@RestoreName IS NULL)
       BEGIN
             SET @RestoreName = @DBName
       END 

       IF (RIGHT(@MoveLocation, 1) = '\')
       BEGIN
             SET @MoveLocation = SUBSTRING(@MoveLocation, 1, LEN(@MoveLocation) - 1)
       END 

       IF (RIGHT(@DownloadPath, 1) = '\')
       BEGIN
             SET @DownloadPath = SUBSTRING(@DownloadPath, 1, LEN(@DownloadPath) - 1)
       END 

       IF (@StopAt IS NOT NULL)
       BEGIN
             SET @StopAt =  'STOPAT = ''' + CONVERT(VARCHAR, @StopAt, 22) + ''''
       END 

       --SELECT @MoveLocation 

       DECLARE @backup_in_s3 BIT = 0, @SQL VARCHAR(MAX) 

       --     full backup
       DECLARE @full_id BIGINT = (SELECT MAX([ID]) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Full' AND  CAST(ExecutionDateTime AS DATE) <= CAST(@EndDate AS DATE))
       DECLARE @full_execution_date DATETIME = (SELECT ExecutionDateTime FROM minion.BackupFiles WHERE [ID] = @full_id)
       DECLARE @full_count INT = (SELECT COUNT(*) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Full' AND ExecutionDateTime = @full_execution_date) 

       --SELECT @full_id, @full_execution_date 

       IF (@full_count > 1)
       BEGIN
             SET @full_id = (SELECT MIN([ID]) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Full' AND ExecutionDateTime = @full_execution_date)
       END 

       --     diff backup
       DECLARE @diff_id BIGINT = (SELECT MAX([ID]) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Diff' AND  CAST(ExecutionDateTime AS DATE) < CAST(@EndDate AS DATE))
       DECLARE @diff_execution_date DATETIME = (SELECT ExecutionDateTime FROM minion.BackupFiles WHERE [ID] = @diff_id)
       --DECLARE @diff_count INT = (SELECT COUNT(*) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Diff' AND ExecutionDateTime = @full_execution_date) 

       /*
       IF (@diff_count > 1)
       BEGIN
             SET @diff_id = (SELECT MIN([ID]) FROM minion.BackupFiles WHERE DBName = @dbName AND BackupType = 'Diff' AND ExecutionDateTime = @full_execution_date)
       END
       */ 

       --SELECT @diff_id, @diff_execution_date 

       SELECT [ID], ExecutionDateTime, BackupType, FullPath, FullFileName, [FileName], [Extension], FirstLSN, LastLSN, BackupStartDate, BackupFinishDate 
       INTO #t
       FROM minion.BackupFiles
       WHERE DBName = @dbName AND [ID] >= @full_id AND ExecutionDateTime <= @EndDate
       ORDER BY [ID] 

       SELECT ExecutionDateTime, FileName, BackupType
       INTO #dates
       FROM #t
	   
       --     delete lingering log record(s)
       DELETE FROM #t WHERE BackupType = 'Log' AND ExecutionDateTime < @diff_execution_date
       DELETE FROM #t WHERE BackupType = 'Log' AND [ID] < @diff_id 

       --     delete lingering diff record(s)
       DELETE FROM #t WHERE BackupType = 'Diff' AND CAST(ExecutionDateTime AS DATE) = CAST(@EndDate AS DATE) 

       --     set vars for last log id and logic check for full
       DECLARE @log_last_id BIGINT = (SELECT TOP 1 [ID] FROM #t WHERE BackupType = 'Log' ORDER BY [ID] DESC)
       DECLARE @full_file_path VARCHAR(2000) = (SELECT TOP 1 FullPath FROM #t WHERE BackupType = 'Full') 

       CREATE TABLE #xp_cmdshell ([id] INT IDENTITY(1,1) PRIMARY KEY NOT NULL, [output] NVARCHAR(255))

       SET @SQL = '
INSERT INTO #xp_cmdshell ([output])
EXEC sys.xp_cmdshell ''dir "' + @full_file_path + '"''
       '
       EXEC (@SQL) 

       --     build s3 file list for download (manual download ATM)

/*     IF EXISTS (SELECT * FROM #xp_cmdshell WHERE [output] LIKE '%File Not Found%')
       BEGIN
             SET @backup_in_s3 = 1 

             DECLARE @backup_filename VARCHAR(1000), @backup_s3_output VARCHAR(MAX) 

             CREATE TABLE #s3_files ([id] INT IDENTITY(1,1) PRIMARY KEY NOT NULL, [keyprefix] VARCHAR(MAX)) 

             --     pull in all file names

             DECLARE cur_s3 CURSOR FOR SELECT [FileName] FROM #t
             OPEN cur_s3
             FETCH NEXT FROM cur_s3 INTO @backup_filename
             WHILE @@FETCH_STATUS = 0
                    BEGIN 
                           IF (SELECT COUNT(*) FROM #s3_files) = 0
                           BEGIN
                                 INSERT INTO #s3_files (keyprefix) SELECT '
$objects = Get-S3Object -BucketName $bucket -KeyPrefix $keyPrefix -AccessKey $accessKey -SecretKey $secretKey -Region $region
foreach($object in $objects) {
       $localFileName = $object.Key -replace $keyPrefix, ''''
       if ($localFileName -ne '''') {
             $localFilePath = Join-Path $localPath $localFileName
             if ($object.key -like ''*' + @backup_filename + '*'''
                           END
                           ELSE
                           BEGIN
						       UPDATE #s3_files SET [keyprefix] = [keyprefix] + ' -or $object.key -like ''*' + @backup_filename + '*'''
                              --SELECT 1
                           END
                           FETCH NEXT FROM cur_s3 INTO @backup_filename
                    END
             CLOSE cur_s3
             DEALLOCATE cur_s3
       END

       --     update to add ending paren + notification
       IF EXISTS (SELECT * FROM #s3_files)
       BEGIN 
             UPDATE #s3_files SET [keyprefix] = [keyprefix] + ') {
                    #write-host $object.Key
                    #Copy-S3Object -BucketName $bucket -Key $object.Key -LocalFile $localFilePath -AccessKey $accessKey -SecretKey $secretKey -Region $region
             }
       }
}
'
             SELECT 'SET the correct S3 keyPrefix (s3 URI) and download ($localPath - @DownloadPath) the following:'
             UNION ALL
             SELECT [keyprefix] FROM #s3_files
             SELECT 'ATTN: FILE(S) NEED TO BE DOWNLOADED FROM S3'
       END

*/   
       IF (@backup_in_s3 = 1)
       BEGIN
             IF (@DownloadPath IS NULL)
             BEGIN
                    SELECT 'SET @DownloadPath'
                    RETURN
             END

             UPDATE #t
             SET FullFileName = REPLACE(FullFileName, SUBSTRING(FullFileName, 0, CHARINDEX(@DBName, FullFileName) + LEN(@DBName)), @DownloadPath)
       END 

       /*

       --     if full backup files > 1
       IF (@full_count > 1)
       BEGIN
             DECLARE @from_disk_multiple NVARCHAR(MAX);
             SELECT @from_disk_multiple = COALESCE(@from_disk_multiple, '') + CHAR(10) + CHAR(13) + 'DISK = ''' + FullFileName + ', '
             FROM #t WHERE BackupType = 'Full'
             SET @from_disk_multiple = STUFF(@from_disk_multiple, CHARINDEX('DISK', @from_disk_multiple), LEN('DISK'), 'FROM DISK')
             88SELECT @from_disk_multiple
       END
       */

       --     create table for SQL statements
       CREATE TABLE #sql ([id] INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, [sql] VARCHAR(MAX)) 

       --     BEGIN full file(s)
       INSERT INTO #sql
       SELECT '--    Full Backup File(s)'
       
	   UNION ALL

       SELECT TOP 1 '--     ExecutionDateTime: ' + CONVERT(VARCHAR, ExecutionDateTime, 120) + ' - BackupFinishDate: ' + CONVERT(VARCHAR, BackupFinishDate, 120) + ' - FirstLSN: ' + FirstLSN + ' - LastLSN: ' + LastLSN
       FROM #t WHERE BackupType = 'Full'

       UNION ALL 

       SELECT TOP 1 '
RESTORE DATABASE [' + @RestoreName + ']
FROM
' 
       FROM #t WHERE BackupType = 'Full'

       UNION ALL 

       --     build full file list
       SELECT '
DISK = ''' + FullFileName + '''' + CASE WHEN @full_count > 1 AND FileName LIKE '%' + CAST(@full_count AS VARCHAR(5)) + 'of' + CAST(@full_count AS VARCHAR(5)) + '%' THEN '' ELSE ',' END
       FROM #t WHERE BackupType = 'Full'

       UNION ALL 

       SELECT TOP 1 '
WITH
MOVE ''' + @DBName + ''' TO ''' + @MoveLocation + '\' + @RestoreName + '.mdf'',
MOVE ''' + @DBName + '_log''' + ' TO ''' + @MoveLocation + '\' + @RestoreName + '.ldf'',
NORECOVERY, REPLACE, STATS = 10;
'      FROM #t WHERE BackupType = 'Full' 

       --     BEGIN diff file(s)
       INSERT INTO #sql
       SELECT '--    Diff Backup File(s)' 

       --    loop through file(s) by execution date for ordering NofN
       DECLARE
             @exec_datetime DATETIME,
             @backup_type VARCHAR(10) = 'Diff' 

       WHILE (1=1)
       BEGIN
             SELECT @exec_datetime = MIN(ExecutionDateTime) FROM #dates WHERE BackupType = @backup_type 

             INSERT INTO #sql
             SELECT '--    ExecutionDateTime: ' + CONVERT(VARCHAR, t.ExecutionDateTime, 120) + ' - BackupFinishDate: ' + CONVERT(VARCHAR, t.BackupFinishDate, 120) + ' - FirstLSN: ' + t.FirstLSN + ' - LastLSN: ' + t.LastLSN + 'RESTORE DATABASE [' + @RestoreName + '] FROM DISK = ''' + t.FullFileName + ''' WITH NORECOVERY, STATS = 10;'
             FROM #t t
             INNER JOIN (SELECT TOP (100) d.ExecutionDateTime, d.FileName, d.BackupType FROM #dates d ORDER BY d.FileName) AS d ON d.FileName = t.FileName AND @exec_datetime = t.ExecutionDateTime
             WHERE t.BackupType = 'Diff'            

             DELETE FROM #dates WHERE ExecutionDateTime = @exec_datetime AND BackupType = @backup_type 

             IF @exec_datetime IS NULL
             BEGIN
                    BREAK;
             END
       END 

       --     BEGIN log file(s)
       INSERT INTO #sql
       SELECT '--    Log Backup File(s)'

       UNION ALL 

       SELECT '--    ExecutionDateTime: ' + CONVERT(VARCHAR, ExecutionDateTime, 120) + ' - BackupFinishDate: ' + CONVERT(VARCHAR, BackupFinishDate, 120) + ' - FirstLSN: ' + FirstLSN + ' - LastLSN: ' + LastLSN + '
RESTORE LOG [' + @RestoreName + '] FROM DISK = ''' + FullFileName + ''' WITH NORECOVERY;
'
       FROM #t WHERE BackupType = 'Log' AND [ID] NOT IN (@log_last_id)

       UNION ALL 

       SELECT '--    ExecutionDateTime: ' + CONVERT(VARCHAR, ExecutionDateTime, 120) + ' - BackupFinishDate: ' + CONVERT(VARCHAR, BackupFinishDate, 120) + ' - FirstLSN: ' + FirstLSN + ' - LastLSN: ' + LastLSN + '
RESTORE LOG [' + @RestoreName + '] FROM DISK = ''' + FullFileName + ''' WITH NORECOVERY' + COALESCE(', ' + CONVERT(VARCHAR, @StopAt, 22), '') + ';
'
       FROM #t WHERE BackupType = 'Log' AND [ID] IN (@log_last_id)

       UNION ALL 

       --     recover
       SELECT '--    Recover Database
RESTORE DATABASE [' + @RestoreName + '] WITH RECOVERY;
' 

SELECT [sql] FROM #sql 

END