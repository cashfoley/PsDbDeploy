$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2

#region License

$LicenseMessage = @"
PsDbDeploy - Powershell Database Deployment for SQL Server Database Updates with coordinated Software releases. 
Copyright (C) 2013-14 Cash Foley Software Consulting LLC
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.
 https://psdbdeploy.codeplex.com/license
"@
#endregion

# ----------------------------------------------------------------------------------

$PublishWhatIf = $false

$DBPatchContext = @{}

$QueuedPatches = New-Object System.Collections.ArrayList

$ScriptsFolder = Resolve-Path (Join-Path $PSScriptRoot 'ClassScripts')
. (Get-ChildItem $ScriptsFolder -Recurse -Filter '*.ps1')

#region Patches_Settings
# ----------------------------------------------------------------------------------
$DefaultConstants = @{
    #-----------------------------------------------------------------------------------------------

    #-----------------------------------------------------------------------------------------------
    BeginTransctionScript = @"
SET XACT_ABORT ON
SET TRANSACTION ISOLATION LEVEL READ COMMITTED
SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
BEGIN TRANSACTION;
"@
# Useful for Debug
# PRINT N'Transaction Count - ' + CAST (@@TRANCOUNT+1 AS varchar) 

    #-----------------------------------------------------------------------------------------------
    EndTransactionScript = @"
IF @@ERROR <> 0 AND @@TRANCOUNT >  0 WHILE @@TRANCOUNT>0 ROLLBACK TRANSACTION;
WHILE @@TRANCOUNT > 0 COMMIT TRANSACTION;
"@

    RollbackTransactionScript = @"
WHILE @@TRANCOUNT>0 ROLLBACK TRANSACTION;
"@

}
#endregion


# ----------------------------------------------------------------------------------
function ExecuteValidators([array]$RegExValidators, $SqlContent)
{ 
    if ($RegExValidators -ne $null)
    {
        $errorFound = $FALSE
        foreach ($RegExValidator in $RegExValidators)
        {
            $ValidatorMatches = $SqlContent -match $RegExValidators
            if ($ValidatorMatches)
            {
#Need Validator Hash Table
                Log-Error $validator.message
                $errorFound = $TRUE
            }
        }
        if ($errorFound)
        {
            Throw "Validators Failed"
        }
    }
}

# ----------------------------------------------------------------------------------
# function Checkpoint-PatchFile( $PatchName, $Comment='Checkpoint' )
# {
#     $PatchFile = Join-Path $PatchContext.RootFolderPath $PatchName
# 
#     $Checksum = FileChecksum $PatchFile
#     
#     MarkPatchAsExecuted $PatchName $Checksum $Comment
# }
# 
# ----------------------------------------------------------------------------------

function ExecutePatchBatch($PatchBatch)
{

}

$PatchController = [PatchController]::new($QueuedPatches)

function Publish-Patches
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]
 
    param () 

    process 
    {
    }
}
Export-ModuleMember -Function Publish-Patches

# ----------------------------------------------------------------------------------

function Add-TokenReplacement ($TokenValue, $ReplacementValue)
{
    $PatchContext.TokenReplacements += @{TokenValue=$TokenValue;ReplacementValue=$ReplacementValue}
}

Export-ModuleMember -Function Add-TokenReplacement

# ----------------------------------------------------------------------------------

function Set-PatchDependency 
    ( [Parameter(Mandatory=$True)]
      $SourcePatch
    , [Parameter(Mandatory=$True)]
      $DependencyPatch
    , [ValidateSet("SourceAfterDependency","DependencyBeforeSource")]
      [string]$Move="DependencyBeforeSource"
    )
{
    function findIndexOfPatch($PatchName)
    {
        $MatchIdx = -1
        for ($idx=0; $idx -lt $QueuedPatches.Count; $idx++)
        {
            if ($QueuedPatches[$idx].PatchName -eq $PatchName)
            {
                $MatchIdx = $idx;
                break;
            }
        }
        if ($MatchIdx -eq -1)
        {
            Write-Verbose "Patch Dependency not set. '$DependencyPatch' not queued"
        }
        $MatchIdx
    }

    if ($Move -eq "DependencyBeforeSource")
    {
        $DependencyIdx = findIndexOfPatch -PatchName $DependencyPatch 
        if ($DependencyIdx -eq -1) {return}  # Not found

        $SourceIdx = findIndexOfPatch -PatchName $SourcePatch
        if ($SourceIdx -eq -1) {return}      # not found

        if ($DependencyIdx -lt $SourceIdx) {return}  #already before

        $savedPatch = $QueuedPatches[$DependencyIdx]
        $QueuedPatches.RemoveAt($DependencyIdx)

        $QueuedPatches.Insert($SourceIdx,$savedPatch)
        Write-Verbose "Moved '$DependencyPatch' before '$SourcePatch'"
    }
    else
    {
        $SourceIdx = findIndexOfPatch -PatchName $SourcePatch
        if ($SourceIdx -eq -1) {return}      # not found

        $DependencyIdx = findIndexOfPatch -PatchName $DependencyPatch 
        if ($DependencyIdx -eq -1) {return}  # Not found

        if ($SourceIdx -gt $DependencyIdx) {return}  #already after

        $savedPatch = $QueuedPatches[$SourceIdx]
        $QueuedPatches.RemoveAt($SourceIdx)

        # Dependency moved down by 1 (because source is always before by rule)
        # Source needs to go to Dependency + 1.  No need to subtract 1 then add 1 for position

        $QueuedPatches.Insert($DependencyIdx,$savedPatch)
        Write-Verbose "Moved '$SourcePatch' after '$DependencyPatch'"
    }
}

Export-ModuleMember -Function Set-PatchDependency


# ----------------------------------------------------------------------------------

function Select-OnFileContent($Pattern)
{
    process
    {
        if ((Get-Content $_.FullName | Out-String) -match $Pattern) {$_}
    }
}

Export-ModuleMember -Function Select-OnFileContent


# ----------------------------------------------------------------------------------


$PatchContext = $null

function Initialize-PsDbDeploy
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]

    param 
    ( $ServerName
    , $DatabaseName
    , $RootFolderPath
    , $Environment
    , $OutFolderPath = (Join-Path $RootFolderPath "OutFolder")
    , [int]$Version
    , [int]$Revision
    , [string]$Comment
    , $SqlLogFile = $null
    , [switch]$PublishWhatif
    , [switch]$EchoSql
    , [switch]$DisplayCallStack
    )

    Write-Verbose $LicenseMessage

    Write-Host "Process DB Deployment for $DatabaseName on server $ServerName"
    Write-Host "    RootFolder: $RootFolderPath"


    if ($SqlLogFile -and (Test-Path $SqlLogFile)) { Remove-Item -Path $SqlLogFile}

    if (! (Test-Path $OutFolderPath -PathType Container) )
    {
        mkdir $OutFolderPath | Out-Null
    }
    
    $script:PatchContext = Get-PatchContext -ServerName $ServerName -DatabaseName $DatabaseName -RootFolderPath $RootFolderPath -OutFolderPath $OutFolderPath -Environment $Environment -DefaultConstants $DefaultConstants -DisplayCallStack $DisplayCallStack
    $PatchContext.LogSqlOutScreen = $EchoSql
    $PatchContext.SqlLogFile = $SqlLogFile
    $PatchContext.PublishWhatIf = $PublishWhatif

    $QueuedPatches = $QueuedPatches.Clear()
    $TokenReplacements = @()

    AssurePsDbDeploy2

    SetDbVersion -Version $Version -Revision $Revision -Comment $Comment

}

Export-ModuleMember -Function Initialize-PsDbDeploy

Export-ModuleMember -Variable DBPatchContext

Export-ModuleMember -Variable PatchContext

Export-ModuleMember -Variable QueuedPatches
