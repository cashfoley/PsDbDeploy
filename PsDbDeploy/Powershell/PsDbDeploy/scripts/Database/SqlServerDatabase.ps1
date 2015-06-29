class SqlServerDatabase : PsDbDatabase
{

    ExecutePsDbDeployUpdate([int]$PsDbDeployVersion)
    {
        Write-Output "Assured"

        $this.ExecuteNonQuery($this._AssurePsDbDeployQuery)

        # ExecuteNonQuery $AssurePsDbDeployQuery
        # $script:PsDbDeployVersion = Get-PsDbDeployVersion
        #         
        # AssurePsDbDeploy2
        $this.PsDbDeployVersion = $PsDbDeployVersion
    }

    hidden [System.Text.RegularExpressions.Regex] $_queriesRegex

    #region SQL and DDL
    hidden [string] $_AssurePsDbDeployQuery = @"
-- Adds PsDbDeploy Objects if they don't exist
--    SCHEMA [PsDbDeploy]
--    TABLE [PsDbDeploy].[FilePatches]
--    PROCEDURE [PsDbDeploy].[MarkPatchExecuted]
--    FUNCTION PsDbDeploy.Version()

BEGIN TRANSACTION;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'PsDbDeploy')
EXEC sys.sp_executesql N'CREATE SCHEMA [PsDbDeploy] AUTHORIZATION [dbo]'
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[FilePatches]') AND type in (N'U'))
BEGIN
CREATE TABLE [PsDbDeploy].[FilePatches](
    [OID] [bigint] IDENTITY(1,1) NOT NULL,
    [FilePath] [nvarchar](450) NOT NULL,
    [Applied] [datetime] NOT NULL,
    [CheckSum] [nvarchar] (100) NOT NULL,
    [Comment] [nvarchar] (4000)
) ON [PRIMARY]
    
CREATE UNIQUE NONCLUSTERED INDEX [UIDX_PsDbDeployFilePatches_FilePath] ON [PsDbDeploy].[FilePatches]
(
    [FilePath] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

END
GO

IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[MarkPatchExecuted]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
    CREATE PROCEDURE [PsDbDeploy].[MarkPatchExecuted]     
        @FilePath [nvarchar](450),
        @CheckSum [nvarchar](100),
        @Comment  [nvarchar](4000)
    AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @OID bigint

        SELECT @OID=OID
            FROM [PsDbDeploy].[FilePatches]
            WHERE FilePath = @FilePath

        IF  (@@ROWCOUNT = 0)
        BEGIN
            INSERT 
                INTO [PsDbDeploy].[FilePatches]
                    ( [FilePath]
                    , [Applied]
                    , [CheckSum]
                    , [Comment])
            VALUES (@FilePath
                    , GetDate()
                    , @CheckSum
                    , @Comment)
        END
        ELSE BEGIN
            UPDATE [PsDbDeploy].[FilePatches]
                SET CheckSum=@CheckSum
                    , Applied=GetDate()
                    , Comment=@Comment
                WHERE OID=@OID
                AND CheckSum<>@CheckSum
        END
    END
' 
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[Version]') AND type = N'FN')
BEGIN
EXEC dbo.sp_executesql @statement = N'
    CREATE FUNCTION PsDbDeploy.Version()
    RETURNS int
    AS
    BEGIN
        RETURN 1
    END
' 
END
GO

COMMIT TRANSACTION;
"@

    #-----------------------------------------------------------------------------------------------
    hidden [string] $_GetPsDbDeployVersionSql = @"
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[Version]') AND type = N'FN')
BEGIN
    SELECT [PsDbDeploy].[Version] ()
END
ELSE BEGIN
    SELECT 0
END
"@

    #endregion
    
    ConnectToServer($ConnectionString)
    {
        $this._connection = [System.Data.SqlClient.SqlConnection]::new()
        $this._connection.ConnectionString = $ConnectionString
        $this._connection.Open()
    }

    SqlServerDatabase( [string]$ServerName, [string]$DatabaseName)
    {

        $this.DatabaseProvider = "SqlServer"

        $QueriesRegexOptions = "IgnorePatternWhitespace,Singleline,IgnoreCase,Multiline,Compiled"
        $QueriesExpression = "((?'Query'(?:(?:/\*.*?\*/)|.)*?)(?:^\s*go\s*$))*(?'Query'.*)"
        $this._queriesRegex = [System.Text.RegularExpressions.Regex]::new($QueriesExpression, [System.Text.RegularExpressions.RegexOptions]$QueriesRegexOptions)

        # Initialize Connection
        $IntegratedConnectionString = 'Data Source={0}; Initial Catalog={1}; Integrated Security=True;MultipleActiveResultSets=False;Application Name="SQL Management"' -f $ServerName,$DatabaseName
        $this.ConnectToServer($IntegratedConnectionString)

        $this.PsDbDeployVersion = $this._getPsDbDeployVersion()
    }

}
