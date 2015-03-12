
#region Schema
$AssurePsDbDeployQuery2 = @"
BEGIN TRANSACTION;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'PsDbDeploy')
BEGIN
    PRINT N'Create SCHEMA PsDbDeploy'

    EXEC sys.sp_executesql N'CREATE SCHEMA [PsDbDeploy] AUTHORIZATION [dbo]'
END

GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[DbVersion]') AND type in (N'U'))
BEGIN
    PRINT N'Creating [PsDbDeploy].[DbVersion]...';

    CREATE TABLE [PsDbDeploy].[DbVersion] (
        [Id]              INT             IDENTITY (1, 1) NOT NULL,
        [Version]         INT             NOT NULL,
        [Revision]        INT             NOT NULL,
        [RollbackTime]    DATETIME        NULL,
        [Comment]         NVARCHAR (4000) NULL
    );

    PRINT N'Creating PK_DbVersion...';

    CREATE UNIQUE CLUSTERED INDEX PK_DbVersion ON PsDbDeploy.DbVersion (Id) 
        WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    PRINT N'Creating IDX_DbVersion_VersionRevision...';
    CREATE UNIQUE NONCLUSTERED INDEX [IDX_DbVersion_VersionRevision] ON [PsDbDeploy].[DbVersion]
    (
	    [Version] ASC,
	    [Revision] ASC
    )
END

GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[DbPatch]') AND type in (N'U'))
BEGIN
    PRINT N'Creating [PsDbDeploy].[DbPatch]...';

    CREATE TABLE [PsDbDeploy].[DbPatch] (
        [Id]              INT             IDENTITY (1, 1) NOT NULL,
        [DbVersionId]     INT             NOT NULL,
        [FilePath]        CHAR (250)      NOT NULL,
        [CheckSum]        CHAR (50)       NOT NULL,
        [Content]         NVARCHAR (MAX)  NOT NULL,
        [RollbackContent] NVARCHAR (MAX)  NULL,
        [RollbackId]      INT             NULL,
        [Invalidated]     BIT             NULL
    );

    PRINT N'Creating PK_DbPatch...';

    CREATE UNIQUE CLUSTERED INDEX PK_DbPatch ON PsDbDeploy.DbPatch (Id) 
        WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    PRINT N'Creating FK_DbPatch_ToFK_DbPatchRollback...';

    ALTER TABLE [PsDbDeploy].[DbPatch] 
        ADD CONSTRAINT [FK_DbPatch_ToFK_DbPatchRollback] FOREIGN KEY ([RollbackId]) REFERENCES [PsDbDeploy].[DbPatch] ([Id]);

    PRINT N'Creating FK_DbPatch_ToDbVersion...';

    ALTER TABLE [PsDbDeploy].[DbPatch] 
        ADD CONSTRAINT [FK_DbPatch_ToDbVersion] FOREIGN KEY ([DbVersionId]) REFERENCES [PsDbDeploy].[DbVersion] ([Id]);
END

GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[DbExecutionLog]') AND type in (N'U'))
BEGIN
    PRINT N'Creating [PsDbDeploy].[DbExecutionLog]...';

    CREATE TABLE [PsDbDeploy].[DbExecutionLog] (
        [Id]                  INT            IDENTITY (1, 1) NOT NULL,
        [DbPatchId]           INT            NOT NULL,
        [ExecutionTime]       DATETIME       NOT NULL,
        [ExecutionSuccessful] BIT            NOT NULL,
        [LogOutput]           NVARCHAR (MAX) NULL
    );

    PRINT N'Creating PK_DbExecutionLog...';

    CREATE UNIQUE CLUSTERED INDEX PK_DbExecutionLog ON PsDbDeploy.DbExecutionLog (Id) 
        WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    PRINT N'Creating FK_DbExecutionLog_ToFK_DbPatch...';

    ALTER TABLE [PsDbDeploy].[DbExecutionLog] WITH NOCHECK
        ADD CONSTRAINT [FK_DbExecutionLog_ToFK_DbPatch] FOREIGN KEY ([DbPatchId]) REFERENCES [PsDbDeploy].[DbPatch] ([Id]);
END
GO

COMMIT TRANSACTION;
"@
#endregion

# ----------------------------------------------------------------------------------
function AssurePsDbDeploy2
{
    NewSqlCommand 
    ExecuteNonQuery $AssurePsDbDeployQuery2
}

export-ModuleMember -Function AssurePsDbDeploy2

$DbVersions = @()
export-ModuleMember -Variable DbVersions

# ----------------------------------------------------------------------------------

$ReadDbVersions = @"
SELECT [Id]
      ,[Version]
      ,[Revision]
      ,[RollbackTime]
      ,[Comment]
  FROM [PsDbDeploy].[DbVersion]
 ORDER BY [ID]
"@

$InsertDbVersion = @"
INSERT INTO [PsDbDeploy].[DbVersion]
           ([Version]
           ,[Revision]
           ,[Comment])
     VALUES
           ({0}
           ,{1}
           ,'{2}'
           )
"@

# ----------------------------------------------------------------------------------
function NewDbVersionObject($Id, $Version,$Revision,$RollbackTime,$Comment)
{
    @{ Id = $Id
       Version = $Version
       Revision = $Revision
       RollbackTime = $RollbackTime
       Comment = $Comment
     }
}

# ----------------------------------------------------------------------------------
function SqlNull2Null($value)
{
    if ($value -is [System.DBNull])
    {
        $null
    }
    else
    {
        $value
    }
}

# ----------------------------------------------------------------------------------
function ReadDbVersions
{
    param ([switch]$Force)
    if ($DbVersions.count -eq 0 -or $Force)
    {
        NewSqlCommand 
        $SqlCommand.CommandText = $ReadDbVersions
        $sqlReader = $SqlCommand.ExecuteReader()
        try
        {
            while ($sqlReader.Read()) 
            { 
                $DbVersion = NewDbVersionObject `
                    -Id $sqlReader["Id"] `
                    -Version $sqlReader["Version"] `
                    -Revision $sqlReader["Revision"] `
                    -RollbackTime (SqlNull2Null $sqlReader["RollbackTime"]) `
                    -Comment (SqlNull2Null $sqlReader["Comment"])

                $script:DbVersions += $DbVersion
            }
        }
        finally
        {
            $sqlReader.Close()
        }
    }
}
export-ModuleMember -Function ReadDbVersions

# ----------------------------------------------------------------------------------
function GetCurrentDbVersion
{
    ReadDbVersions
    $DbVersions | ?{!$_.RollbackTime} | Select-Object -Last 1
}
export-ModuleMember -Function GetCurrentDbVersion

# ----------------------------------------------------------------------------------
function GetMaxDbVersion
{
    ReadDbVersions
    $DbVersion = GetCurrentDbVersion

    if ($DbVersion)
    {
        $Version = $DbVersion.Version
        $Revision = 0
        $DbVersions | ?{$Version -lt $_.Version} | %{$Version = $_.Version}
        $DbVersions | ?{$Version -eq $_.Version -and $Revision -lt $_.Revision} | %{$DbVersion = $_}
        $DbVersion
    }
}
export-ModuleMember -Function GetMaxDbVersion

# ----------------------------------------------------------------------------------
function GetDbVersion($Version,$Revision)
{
    ReadDbVersions
    $DbVersions | ?{$_.Version -eq $Version -and $_.Revision -eq $Revision} | Select-Object -Last 1
}
export-ModuleMember -Function GetDbVersion

# ----------------------------------------------------------------------------------
function SetDbVersion
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]
    param
    ( [int] $Version
    , [int] $Revision
    , [string] $Comment
    )

    ReadDbVersions -Force
    $MaxVersion = GetMaxDbVersion

    if (!$MaxVersion)  
    {
        # No existing versions
        if (!$Version) {$Version = 1}
        if (!$Revision) {$Revision = 0}
    }
    else
    {
        if (!$Version)
        {
            $Version = $MaxVersion.Version
        }
        elseif ($Version -lt $MaxVersion.Version)
        {
            Throw "Version cannot be less then current Version of $($MaxVersion.Version)"
        }

        if (!$Revision)
        {
            $Revision = 0
            if ($Version -eq $MaxVersion.Version)
            {
                $Revision = $MaxVersion.Revision + 1
            }
            else
            {
                $DbVersions | ?{$Version -eq $_.Version -and $Revision -le $_.Revision} | %{$Revision = $_.Revision+1}
            }
        }
        elseif ($Revision -lt $MaxVersion.Revision)
        {
            Throw "Revision must be greater than or equal to current Revision of $($MaxVersion.Revision)"
        }
    }

    $CurrentDbVersion = GetCurrentDbVersion
    if (!$CurrentDbVersion -or $Version -ne $CurrentDbVersion.Version -or $Revision -ne $CurrentDbVersion.Revision)
    {
        NewSqlCommand ($InsertDbVersion -f $Version,$Revision,($Comment.Replace("'","''")))
        [void] $SqlCommand.ExecuteNonQuery()
        $SqlCommand.CommandText = "SELECT @@IDENTITY"
        $ID = $SqlCommand.ExecuteScalar()

        $NewVersion = NewDbVersionObject -Id $id -Version $Version -Revision $Revision -Comment $Comment
        $Script:DbVersions += $NewVersion
    }
}

export-ModuleMember -Function SetDbVersion

# ----------------------------------------------------------------------------------
$InsertDbPatch = @"
DECLARE @RollbackID int
SELECT @RollbackID=MAX(ID) FROM [PsDbDeploy].[DbPatch] WHERE [FilePath] = '{0}' AND [Invalidated] IS NULL AND {3} = 1

DECLARE @NewDbPatchId int

INSERT INTO [PsDbDeploy].[DbPatch]
          ( [DbVersionId]
          , [FilePath]
          , [CheckSum]
          , [Content]
          , [RollbackId]
          , [RollbackContent]
          )
     VALUES
          ( (SELECT MAX(ID) FROM [PsDbDeploy].[DbVersion] WHERE [RollbackTime] IS NULL)
          , '{0}'
          , '{1}'
          , '{2}'
          , @RollbackID
            -- If using RollbackID for this patch, only insert RollbackContent if no previous id
          , (SELECT '{4}' WHERE {3} = 0 OR @RollbackID IS NULL)
          )

SELECT @NewDbPatchId=@@IDENTITY

"@

function New-DbPatch($FilePath, $Checksum, [string]$Content, [switch]$RollBackToPatch, [string]$RollbackConent)
{
    if($RollBackToPatch){$RollbackFlag=1}else{$RollbackFlag=0}

    $pachSql = $InsertDbPatch -f $FilePath,$Checksum.Replace("'","''"),$Content.Replace("'","''"),$RollbackFlag,$RollbackConent.Replace("'","''")
    NewSqlCommand $pachSql
    [void] $SqlCommand.ExecuteNonQuery()
}
export-ModuleMember -Function New-DbPatch

# ----------------------------------------------------------------------------------
$InsertExecutionLog = @"

INSERT INTO [PsDbDeploy].[DbExecutionLog]
          ( [DbPatchId]
          , [ExecutionTime]
          , [ExecutionSuccessful]
          , [LogOutput])
     VALUES
          ( (SELECT MAX(ID) FROM [PsDbDeploy].[DbPatch] WHERE [FilePath] = '{0}' AND [Invalidated] IS NULL)
          , GETDATE()
          , {1}
          , '{2}'
          )

"@

function New-DbExecutionLog($FilePath, [switch]$Successful, [string]$LogOutput)
{
    if($Successful){$SuccessfulFlag=1}else{$SuccessfulFlag=0}

    $ExecutionLogSql = $InsertExecutionLog -f $FilePath,$SuccessfulFlag,$LogOutput.Replace("'","''")
    NewSqlCommand $ExecutionLogSql
    
    [void] $SqlCommand.ExecuteNonQuery() 
}
export-ModuleMember -Function New-DbExecutionLog

# ----------------------------------------------------------------------------------

