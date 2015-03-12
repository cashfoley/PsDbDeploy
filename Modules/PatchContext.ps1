
# ----------------------------------------------------------------------------------
# This fuction takes a string or an array of strings and parses SQL blocks
# Separated by 'GO' statements.   Go Statements must be the only word on
# the line.  The parser ignores GO statements inside /* ... */ comments.
function ParseSqlStrings ($SqlStrings)
{
    $SqlString = $SqlStrings | Out-String

    $SqlQueries = $QueriesRegex.Matches($SqlString)
    foreach ($capture in $SqlQueries[0].Groups["Query"].Captures)
    {
        $capture.Value | ?{($_).trim().Length -gt 0}  # don't return empty strings
    }
}

# ----------------------------------------------------------------------------------
function LogExecutedSql($SqlString)
{
    if ($LogSqlOutScreen)
    {
        $SqlString,"GO" | Write-Output 
    }
    if ($SqlLogFile)
    {
        $SqlString,"GO" | Add-Content -Path $SqlLogFile
    }
}
        
#region Constants
$QueriesRegexOptions = "IgnorePatternWhitespace,Singleline,IgnoreCase,Multiline,Compiled"
$QueriesExpression = "((?'Query'(?:(?:/\*.*?\*/)|.)*?)(?:^\s*go\s*$))*(?'Query'.*)"
$QueriesRegex = New-Object System.Text.RegularExpressions.Regex -ArgumentList ($QueriesExpression, [System.Text.RegularExpressions.RegexOptions]$QueriesRegexOptions)

$AssurePsDbDeployQuery = @"
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
$GetPsDbDeployVersion = @"
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PsDbDeploy].[Version]') AND type = N'FN')
BEGIN
SELECT [PsDbDeploy].[Version] ()
END
ELSE BEGIN
SELECT 0
END
"@

#-----------------------------------------------------------------------------------------------
    
$ChecksumForPatchQuery = @"
SELECT CheckSum
FROM [PsDbDeploy].[FilePatches]
WHERE FilePath = @FilePath
"@

#-----------------------------------------------------------------------------------------------
$MarkPatchAsExecutedQuery = @"
EXEC [PsDbDeploy].[MarkPatchExecuted] @FilePath,@CheckSum,@Comment
"@

# ----------------------------------------------------------------------------------
function AssurePsDbDeploy
{
    if ($PsDbDeployVersion -lt $ThisPsDbDeployVersion)
    {
        NewSqlCommand
        ExecuteNonQuery $AssurePsDbDeployQuery
        $script:PsDbDeployVersion = Get-PsDbDeployVersion
                
        AssurePsDbDeploy2
    }
}
export-ModuleMember -Function AssurePsDbDeploy
# ----------------------------------------------------------------------------------
function Get-PsDbDeployVersion
{
    NewSqlCommand $GetPsDbDeployVersion
    $SqlCommand.ExecuteScalar()
}

# ----------------------------------------------------------------------------------
# function Get-ChecksumForPatch($filePath)
# {
#     if ($PsDbDeployVersion -gt 0)
#     {
#         NewSqlCommand $ChecksumForPatchQuery
#         ($SqlCommand.Parameters.Add("@FilePath",$null)).value = $filePath
#         $SqlCommand.ExecuteScalar()
#     }
#     else
#     {
#         ''
#     }
# }


# ----------------------------------------------------------------------------------
        
function TerminalError($Exception,$OptionalMsg)
{
    $ExceptionMessage = $Exception.Exception.Message;
    if ($Exception.Exception.InnerException)
    {
        $ExceptionMessage = $Exception.Exception.InnerException.Message;
    }
    $errorQueryMsg = "`n{0}`n{1}" -f $ExceptionMessage,$OptionalMsg
    Write-PsDbDeployLog -Error $errorQueryMsg
    
    if ($PsDbDsiplayCallStack)
    {
        $brkline = '=========================================================================='
            Write-PsDbDeployLog -Error $brkline
            Write-PsDbDeployLog -Error 'Stack calls'
            Write-PsDbDeployLog -Error $brkline
        $stack = Get-PSCallStack

            Write-PsDbDeployLog -Error "Location: $($Exception.InvocationInfo.PositionMessage)"
            Write-PsDbDeployLog -Error " Command: $($stack[1].Command)"
        # Write-PsDbDeployLog -Error "Position: $($Exception.InvocationInfo.Line)"
            Write-PsDbDeployLog -Error $brkline

        for ($i = 1; $i -lt $stack.Count; $i++)
        #foreach ($stackItem in $stack)
        {
            $stackItem = $stack[$i]
            Write-PsDbDeployLog -Error "Location: $($stackItem.Location)"
            Write-PsDbDeployLog -Error " Command: $($stackItem.Command)"
            Write-PsDbDeployLog -Error "Position: $($stackItem.Position)"
            Write-PsDbDeployLog -Error $brkline
        }
    }
    Exit
}

Export-ModuleMember -Function TerminalError 

# ----------------------------------------------------------------------------------
$ShaProvider = New-Object "System.Security.Cryptography.SHA1CryptoServiceProvider"
$ChecksumPattern = "{0} {1:d7}"
function GetFileChecksum ([System.IO.FileInfo] $fileInfo)
{
    $file = New-Object "system.io.FileStream" ($fileInfo, [system.io.filemode]::Open, [system.IO.FileAccess]::Read)
        
    try
    {
        $shaHash = [system.Convert]::ToBase64String($ShaProvider.ComputeHash($file))  
        $ChecksumPattern -f $shaHash,$fileInfo.Length
    }
    finally 
    {
        $file.Close()
    }
}
Export-ModuleMember -Function GetFileChecksum 

# ----------------------------------------------------------------------------------
function GetStringChecksum ([string] $StringValue)
{
    $StringBuilder = New-Object System.Text.StringBuilder 
    $ShaProvider.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($StringValue)) | `
        %{[Void]$StringBuilder.Append($_.ToString("x2"))}
    "{0} {1:d7}" -f $StringBuilder.ToString(),$StringValue.Length
}
Export-ModuleMember -Function GetStringChecksum 

# ----------------------------------------------------------------------------------
function GetChecksumForPatch($filePath)
{
    if ($PsDbDeployVersion -gt 0)
    {
        NewSqlCommand $ChecksumForPatchQuery
        ($SqlCommand.Parameters.Add("@FilePath",$null)).value = $filePath
        $SqlCommand.ExecuteScalar()
    }
    else
    {
        ''
    }
}
Export-ModuleMember -Function GetChecksumForPatch 

# ----------------------------------------------------------------------------------
function GetPatchName( [string]$PatchFile )
{
    if (! $Patchfile.StartsWith($RootFolderPath))
    {
        Throw ("Patchfile '{0}' not under RootFolder '{1}'" -f $PatchFile,$RootFolderPath)
    }
    $PatchFile.Replace($RootFolderPath, '')
}
Export-ModuleMember -Function GetPatchName 

# ----------------------------------------------------------------------------------
        
function NewSqlCommand($CommandText='')
{
    $NewSqlCmd = $Connection.CreateCommand()
    $NewSqlCmd.CommandTimeout = $DefaultCommandTimeout
    $NewSqlCmd.CommandType = [System.Data.CommandType]::Text
    $NewSqlCmd.CommandText = $CommandText
    $Script:SqlCommand = $NewSqlCmd
}
Export-ModuleMember -Function NewSqlCommand 

# ----------------------------------------------------------------------------------

function Get-DBServerName
{
    $DBServerName
}

# ----------------------------------------------------------------------------------

function Get-DatabaseName
{
    $DatabaseName
}
Export-ModuleMember -Function Get-DatabaseName 

# ----------------------------------------------------------------------------------
function ExecuteNonQuery($Query,[switch]$DontLogErrorQuery,[string]$ErrorMessage)
{
    $ParsedQueries = ParseSqlStrings $Query
    foreach ($ParsedQuery in $ParsedQueries)
    {
        if ($ParsedQuery.Trim() -ne "")
        {
            LogExecutedSql $ParsedQuery
            if (! $PublishWhatIf)
            {
                try
                {
                    $SqlCommand.CommandText=$ParsedQuery
                    [void] $SqlCommand.ExecuteNonQuery()
                } 
                catch
                {
                    TerminalError $_ $ParsedQuery
                }
            }
        }
    }
}
Export-ModuleMember -Function ExecuteNonQuery 

# ----------------------------------------------------------------------------------
function GetMarkPatchAsExecutedString($filePath, $Checksum, $Comment)
{
    "EXEC [PsDbDeploy].[MarkPatchExecuted] N'{0}',N'{1}',N'{2}'" -f $filePath.Replace("'","''"),$Checksum.Replace("'","''"),$Comment.Replace("'","''")
}
Export-ModuleMember -Function GetMarkPatchAsExecutedString 

# ----------------------------------------------------------------------------------
function MarkPatchAsExecuted($filePath, $Checksum, $Comment)
{
    ExecuteNonQuery (GetMarkPatchAsExecutedString -filepath $filePath -checksum $Checksum -comment $Comment )
}
Export-ModuleMember -Function MarkPatchAsExecuted 

# ----------------------------------------------------------------------------------
Function ParseSchemaAndObject($SourceStr, $ParseRegex)
{
    function isNumeric ($x) {
        $x2 = 0
        $isNum = [System.Int32]::TryParse($x, [ref]$x2)
        return $isNum
    }

    $GotMatches = $SourceStr -match $ParseRegex
    $ParesedOwner = @{}
    if ($GotMatches)
    {
        foreach ($key in $Matches.Keys)
        {
            if (!(isNumeric $key))
            {
                $ParesedOwner[$key] =$Matches[$key]
            }
        }
    }

    $GotMatches,$ParesedOwner
}
Export-ModuleMember -Function ParseSchemaAndObject 

# ----------------------------------------------------------------------------------
function ReplacePatternValues($text,$MatchSet)
{
    foreach ($key in $MatchSet.Keys)
    {
        $source = '@(' + $key + ')'
        $text = $text.Replace($source, $MatchSet[$key])
    }
    $text
}

Export-ModuleMember -Function ReplacePatternValues 

# ----------------------------------------------------------------------------------
function NewPatchObject($Patcher,$PatchFile,$PatchName,$CheckPoint,$Comment,$NoTransaction=$false,$ExecuteOnce=$false,$force=$false)
{
    $patchContent = ([System.IO.File]::OpenText($PatchFile).readtoend())
    $patchChecksum = GetStringChecksum $patchContent
    $patch = New-Object -TypeName PSObject -Property (@{
        Patcher = $Patcher
        PatchFile = $PatchFile
        PatchName = $PatchName
        Comment = $Comment
        CheckPoint = $CheckPoint
        Ignore = $false
        NoTransaction = $NoTransaction
        PatchContent =  $patchContent
        CheckSum = $patchChecksum
        ExecuteOnce = $ExecuteOnce
        Force = $force
        #BeforeEachPatch = $BeforeEachPatch
        #AfterEachPatch = $AfterEachPatch
        PatchAttributes = @{}
        #ErrorException = $null
        }) 

    function setPatchFlag ($patch, $parsedPatchName, $flagName)
    {
        if ($Patch.PatchName -eq $parsedPatchName)
        {
            switch ($flagName)
            {
                'Ignore'        
                {
                    $patch.Ignore = $true
                        Write-Verbose "Patch Flag $flagName Applied."
                        break;
                }
                'NoTransaction' 
                {
                    $patch.NoTransaction = $true
                        Write-Verbose "Patch Flag $flagName Applied."
                        break;
                }
                'ExecuteOnce'   
                {
                    $patch.ExecuteOnce = $true
                        Write-Verbose "Patch Flag $flagName Applied."
                        break;
                }
                'Force'         {$patch.ExecuteOnce = $true}
                Default {Throw "Invalid PsDbDeploy Patch Flag $flagName"}
            }
        }
        else
        {
            Write-Verbose "Patch Flag $flagName not used.  $parsedPatchName is for different file."
        }
    }

    $flagPattern = "\[(?'ParsedPatchName'.*?)]::(?'Flag'\w+)"

    $flags = [System.Text.RegularExpressions.Regex]::Matches($patchContent,$flagPattern)
    $flags | %{setPatchFlag $patch -parsedPatchName $_.Groups['ParsedPatchName'].Value -flagName $_.Groups['Flag'].Value }

    $patch
}	
        
Export-ModuleMember -Function NewPatchObject 

# ----------------------------------------------------------------------------------
function TestEnvironment([System.IO.FileInfo]$file)
{
    # returns false if the basename ends with '(something)' and something doesn't match $Environment or if it is $null
    if ($file.basename -match ".*?\((?'fileEnv'.*?)\)$")
    {
        ($Matches['fileEnv'] -ne $Environment)
    }
    else
    {
        $true
    }
}

Export-ModuleMember -Function TestEnvironment 

#endregion
        

$DBServerName = $null
$DatabaseName = $null
$DefaultCommandTimeout = 180
$RootFolderPath = $null
$Connection = $null
$SqlCommand = $null
$PsDbDsiplayCallStack = $false

$ThisPsDbDeployVersion = 1
$PsDbDeployVersion = $null
$LogSqlOutScreen = $false
$SqlLogFile = $null
$PublishWhatif = $false
$Environment = $null

function Set-DbPatchContext ($ServerName, $DatabaseName, $RootFolderPath, [switch]$DisplayCallStack, $Environment)
{
    $Script:PsDbDsiplayCallStack = $DisplayCallStack

    # ----------------------------------------------------------------------------------
    $Script:DBServerName = $ServerName
    $Script:DatabaseName = $DatabaseName
        
    if (!(Test-Path $RootFolderPath -PathType Container))
    {
        Throw 'RootFolder is not folder - $RootFolderPathParm'
    }
        
    $Script:RootFolderPath = Join-Path $RootFolderPath '\'  # assure consitent \ on root folder name

    # Initialize Connection
    $ConnectionString = 'Data Source={0}; Initial Catalog={1}; Integrated Security=True;MultipleActiveResultSets=False;Application Name="SQL Management"'
    $Script:Connection = (New-Object "System.Data.SqlClient.SqlConnection")
    $Connection.ConnectionString = $ConnectionString -f $DBServerName,$DatabaseName
    $Connection.Open()

    ## Attach the InfoMessage Event Handler to the connection to write out the messages 
    $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
        param($sender, $event) 
        #Write-PsDbDeployLog "----------------------------------------------------------------------------------------"
        # $event | FL
        Write-PsDbDeployLog "    >$($event.Message)"
    };
         
    $Connection.add_InfoMessage($handler); 
    $Connection.FireInfoMessageEventOnUserErrors = $false;

    $Script:SqlCommand = NewSqlCommand

    $Script:PsDbDeployVersion = Get-PsDbDeployVersion
    $Script:Environment = $Environment
}



