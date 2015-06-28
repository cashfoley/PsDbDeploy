$ErrorActionPreference = "stop"


class PsDbDatabase
{
    $DatabaseProvider = "Undefined"
    $DefaultCommandTimeout = 100


    hidden [System.Data.Common.DbConnection] $_connection

    # defined in abstract.  However, because it is used to track the necessity
    # of applying updates to the PsDbDeploy schema, the value is managed by the
    # Database specific class
    [int] $PsDbDeployVersion

    [string] ConnectionInfo()
    {
        $sb = [System.Text.StringBuilder]::new() 
        $sb.AppendLine("  DatabaseProvider: $($this.DatabaseProvider)")
        $sb.AppendLine(" PsDbDeployVersion: $($this.PsDbDeployVersion)")
        $sb.AppendLine("        ServerName: $($this._connection.DataSource)")
        $sb.AppendLine("      DatabaseName: $($this._connection.Database)")
        $sb.AppendLine("  Connection State: $($this._connection.State)")

        return $sb.ToString()
    }

    hidden [System.Data.Common.DbCommand] _newSqlCommand()
    {
        [System.Data.Common.DbCommand]$NewSqlCmd = $this._connection.CreateCommand()

        $NewSqlCmd.CommandTimeout = $this.DefaultCommandTimeout
        $NewSqlCmd.CommandType = [System.Data.CommandType]::Text

        return $NewSqlCmd
    }

    hidden [System.Data.Common.DbCommand] _newSqlCommand($CommandText='')
    {
        $NewSqlCmd = $this._newSqlCommand()

        $NewSqlCmd.CommandText = $CommandText

        return $NewSqlCmd
    }


    hidden _AssurePsDbDeploy([int]$PsDbDeployVersion)
    {
        if ($PsDbDeployVersion -gt $this.PsDbDeployVersion)
        {
            $this.ExecutePsDbDeployUpdate($PsDbDeployVersion)
        }
    }

    ExecutePsDbDeployUpdate([int]$PsDbDeployVersion)
    {
        throw "Not Implemented"
    }

    hidden [int] _getPsDbDeployVersion()
    {
        $SqlCommand = $this._newSqlCommand($this._GetPsDbDeployVersionSql)
        $version = $SqlCommand.ExecuteScalar()
        return $version
    }

    LogExecutedSql($query)
    {

    }

    [bool] $PublishWhatIf = $false
    [bool] $DisplayCallStack = $true

    TerminalError($Exception,$OptionalMsg)
    {
        $sb = [System.Text.StringBuilder]::new() 

        $ExceptionMessage = $Exception.Exception.Message;
        if ($Exception.Exception.InnerException)
        {
            $ExceptionMessage = $Exception.Exception.InnerException.Message;
        }
        $errorQueryMsg = "`n{0}`n{1}" -f $ExceptionMessage,$OptionalMsg
        $sb.AppendLine("$errorQueryMsg")

    
        if ($this.DisplayCallStack)
        {
            $brkline = '=========================================================================='
            $sb.AppendLine("$brkline")
            $sb.AppendLine("$errorQueryMsg")
            $sb.AppendLine('Stack calls')
            $sb.AppendLine("$brkline")

            $stack = Get-PSCallStack

            $sb.AppendLine("Location: $($Exception.InvocationInfo.PositionMessage)")
            $sb.AppendLine(" Command: $($stack[1].Command)")
            #$sb.AppendLine("Position: $($Exception.InvocationInfo.Line)")
            $sb.AppendLine($brkline)

            for ($i = 1; $i -lt $stack.Count; $i++)
            #foreach ($stackItem in $stack)
            {
                $stackItem = $stack[$i]
                $sb.AppendLine("Location: $($stackItem.Location)")
                $sb.AppendLine(" Command: $($stackItem.Command)")
                $sb.AppendLine("Position: $($stackItem.Position)")
                $sb.AppendLine($brkline)
            }
        }
        Write-Output $sb.ToString()
        Exit
    }

    [string[]] ParseSqlStrings ($SqlStrings)
    {
        $SqlString = $SqlStrings | Out-String

        $results = @()

        $SqlQueries = $this._queriesRegex.Matches($SqlString)
        foreach ($capture in $SqlQueries[0].Groups["Query"].Captures)
        {
            $results += $capture.Value | ?{($_).trim().Length -gt 0}  # don't return empty strings
        }
        return $results
    }


    ExecuteNonQuery($Query)
    {
        $cmd = $this._newSqlCommand()

        $ParsedQueries = $this.ParseSqlStrings($Query)
        foreach ($ParsedQuery in $ParsedQueries)
        {
            if ($ParsedQuery.Trim() -ne "")
            {
                $this.LogExecutedSql($ParsedQuery)
                if (! $this.PublishWhatIf)
                {
                    try
                    {
                        $cmd.CommandText=$ParsedQuery
                        [void] $cmd.ExecuteNonQuery()
                    } 
                    catch
                    {
                        $this.TerminalError($_,$ParsedQuery)
                    }
                }
            }
        }
    }
}

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


$sql = [SqlServerDatabase]::new(".","testdb")

$sql | fl

$sql.ConnectionInfo()
$sql._AssurePsDbDeploy(1)



