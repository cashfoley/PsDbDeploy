
function Get-PatchContext ($ServerName, $DatabaseName, $RootFolderPath, $OutFolderPath, $DefaultConstants, [switch]$DisplayCallStack, $Environment)
{
    New-Module -AsCustomObject -ArgumentList $ServerName, $DatabaseName, $RootFolderPath, $OutFolderPath, $DefaultConstants, $DisplayCallStack, $Environment -ScriptBlock {
        param
        ( $DBServerNameParm
        , $DatabaseNameParm
        , $RootFolderPathParm
        , $OutFolderPathParm
        , $DefaultConstants
        , $DisplayCallStackParm
        , $EnvironmentParm
        )
        $ErrorActionPreference = "Stop"
        Set-StrictMode -Version 2

        $DisplayCallStack = $DisplayCallStackParm

        #region Private Functions
        # ----------------------------------------------------------------------------------

        $QueriesRegexOptions = "IgnorePatternWhitespace,Singleline,IgnoreCase,Multiline,Compiled"
        $QueriesExpression = "((?'Query'(?:(?:/\*.*?\*/)|.)*?)(?:^\s*go\s*$))*(?'Query'.*)"
        $QueriesRegex = New-Object System.Text.RegularExpressions.Regex -ArgumentList ($QueriesExpression, [System.Text.RegularExpressions.RegexOptions]$QueriesRegexOptions)

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
        #endregion
        
        #region Exported Functions
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
            $host.ui.WriteErrorLine($errorQueryMsg) 
    
            if ($DisplayCallStack)
            {
                $brkline = '=========================================================================='
                $host.ui.WriteErrorLine($brkline)
                $host.ui.WriteErrorLine('Stack calls')
                $host.ui.WriteErrorLine($brkline)
                $stack = Get-PSCallStack

                $host.ui.WriteErrorLine("Location: $($Exception.InvocationInfo.PositionMessage)")
                $host.ui.WriteErrorLine(" Command: $($stack[1].Command)")
                #$host.ui.WriteErrorLine("Position: $($Exception.InvocationInfo.Line)")
                $host.ui.WriteErrorLine($brkline)

                for ($i = 1; $i -lt $stack.Count; $i++)
                #foreach ($stackItem in $stack)
                {
                   $stackItem = $stack[$i]
                   $host.ui.WriteErrorLine("Location: $($stackItem.Location)")
                   $host.ui.WriteErrorLine(" Command: $($stackItem.Command)")
                   $host.ui.WriteErrorLine("Position: $($stackItem.Position)")
                   $host.ui.WriteErrorLine($brkline)
                }
            }
            Exit
        }

        Export-ModuleMember -Function TerminalError 

        # ----------------------------------------------------------------------------------
        $ShaProvider = New-Object "System.Security.Cryptography.SHA1CryptoServiceProvider"
        $MD5Provider = New-Object "System.Security.Cryptography.MD5CryptoServiceProvider"
        function GetFileChecksum ([System.IO.FileInfo] $fileInfo)
        {
            $file = New-Object "system.io.FileStream" ($fileInfo, [system.io.filemode]::Open, [system.IO.FileAccess]::Read)
        
            try
            {
                $shaHash = [system.Convert]::ToBase64String($ShaProvider.ComputeHash($file))  
                #$file.Position =0
                #$md5Hash = [system.Convert]::ToBase64String($MD5Provider.ComputeHash($file))  

                #Sample: md5:'KJ5/LZAAzMmOzHn7rowksg==' sha:'LNa8s47m0pa8BUPmy8QNQsc/vdc=' length:006822
                #"md5:'{0}' sha:'{1}' length:{2:d6}" -f $md5Hash,$shaHash,$fileInfo.Length
                
                "{0} {1:d7}" -f $shaHash,$fileInfo.Length
            }
            finally 
            {
                $file.Close()
            }
        }
        Export-ModuleMember -Function GetFileChecksum 

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
        Export-ModuleMember -Function Get-DBServerName 

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
        function NewPatchObject($Patcher,$PatchFile,$PatchName,$Checksum,$CheckPoint,$Comment)
        {
            New-Object -TypeName PSObject -Property (@{
                Patcher = $Patcher
                PatchFile = $PatchFile
                PatchName = $PatchName
                CheckSum = $CheckSum
                Comment = $Comment
                CheckPoint = $CheckPoint
                PatchContent = Get-Content $PatchFile | Out-String
                #BeforeEachPatch = $BeforeEachPatch
                #AfterEachPatch = $AfterEachPatch
                PatchAttributes = @{}
                #ErrorException = $null
                }) 
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

        # ----------------------------------------------------------------------------------
        function OutPatchFile($Filename,$Content)
        {
            $script:OutPatchCount += 1
            $outFileName = "{0:0000}-{1}" -f $OutPatchCount, ($Filename.Replace("\","-").Replace("/","-"))
            $Content | Set-Content -Path (Join-Path $OutFolderPath $outFileName)
        }

        Export-ModuleMember -Function OutPatchFile 

        #endregion
        
        # ----------------------------------------------------------------------------------
        $DBServerName = $DBServerNameParm
        $DatabaseName = $DatabaseNameParm
        $DefaultCommandTimeout = 180
        
        if (!(Test-Path $RootFolderPathParm -PathType Container))
        {
            Throw 'RootFolder is not folder - $RootFolderPathParm'
        }
        
        $RootFolderPath = Join-Path $RootFolderPathParm '\'  # assure consitent \ on root folder name
        Export-ModuleMember -Variable RootFolderPath 

        $Constants = $DefaultConstants
        Export-ModuleMember -Variable Constants 
        
        # Initialize Connection
        $IntegratedConnectionString = 'Data Source={0}; Initial Catalog={1}; Integrated Security=True;MultipleActiveResultSets=False;Application Name="SQL Management"'
        $Connection = (New-Object "System.Data.SqlClient.SqlConnection")
        $Connection.ConnectionString = $IntegratedConnectionString -f $DBServerName,$DatabaseName
        $Connection.Open()
        Export-ModuleMember -Variable Connection 

        ## Attach the InfoMessage Event Handler to the connection to write out the messages 
        $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
            param($sender, $event) 
            #Write-Host "----------------------------------------------------------------------------------------"
            $event | FL
            Write-Host "    >$($event.Message)"
        };
         
        $Connection.add_InfoMessage($handler); 
        $Connection.FireInfoMessageEventOnUserErrors = $false;

        $SqlCommand = NewSqlCommand

        $Patchers = @()

        $TokenReplacements = @()
        Export-ModuleMember -Variable TokenReplacements

        $OutFolderPath = Join-Path $OutFolderPathParm (get-date -Format yyyy-MM-dd-HH.mm.ss.fff)
        if (! (Test-Path $OutFolderPath -PathType Container) )
        {
            mkdir $OutFolderPath | Out-Null
        }

        $OutPatchCount = 0
    
        $ThisPsDbDeployVersion = 1
        $PsDbDeployVersion = Get-PsDbDeployVersion
        $LogSqlOutScreen = $false
        $SqlLogFile = $null
        $PublishWhatif = $false
        $Environment = $EnvironmentParm

        Export-ModuleMember -Variable SqlCommand 
        Export-ModuleMember -Variable LogSqlOutScreen 
        Export-ModuleMember -Variable PublishWhatif 
        Export-ModuleMember -Variable SqlLogFile 
        Export-ModuleMember -Variable OutFolderPath
        Export-ModuleMember -Variable OutPatchCount
        Export-ModuleMember -Variable Environment 
        Export-ModuleMember -Function NewCommand
    }
}



