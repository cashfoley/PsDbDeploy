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

. (Join-Path $PSScriptRoot "PsDbSqlDeploy.ps1")
. (Join-Path $PSScriptRoot "LoadXmlData.ps1")
. (Join-Path $PSScriptRoot "PatchContext.ps1")
. (Join-Path $PSScriptRoot "VersionManagement.ps1")

#region Patches_Settings
# ----------------------------------------------------------------------------------
$Constants = @{
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

$OutPatchCount = 0
$OutFolderPath = $null

function OutPatchFile($Filename,$Content)
{
    $script:OutPatchCount += 1
    $outFileName = "{0:0000}-{1}" -f $OutPatchCount, ($Filename.Replace("\","-").Replace("/","-"))
    $Content | Set-Content -Path (Join-Path $OutFolder $outFileName)
}



# ----------------------------------------------------------------------------------
function Checkpoint-PatchFile( $PatchName, $Comment='Checkpoint' )
{
    $PatchFile = Join-Path $RootFolderPath $PatchName
    
    $Checksum = GetFileChecksum (Get-ChildItem $PatchFile) 
    
    MarkPatchAsExecuted $PatchName $Checksum $Comment 
}

Export-ModuleMember -Function Checkpoint-PatchFile

# ----------------------------------------------------------------------------------

function ExecutePatchBatch($PatchBatch)
{

}

try
{
}
catch
{
}

function Write-PsDbDeployLog($Message,[switch]$error)
{
    if ($error)
    {
        #$host.ui.WriteErrorLine($errorQueryMsg) 
        Write-Verbose ("ERROR: " + $Message)
    }
    else
    {
        Write-Verbose $Message
    }
}
Export-ModuleMember -Function Write-PsDbDeployLog

function CallPatcher ($Patcher,$Patches,$WhatIfExecute)
{
    if ($patcher.PatchType = 'SQL')
    {
        Perform-SqlPatches $Patches -WhatIfExecute $WhatIfExecute
    }
    else
    {
        throw "unimplementd patcher"
    }
}

function Publish-Patches
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]
 
    param () 
    begin
    {
        $Script:OutPatchCount = 0
        $Script:OutFolder = Join-Path $OutFolderPath (get-date -Format yyyy-MM-dd-HH.mm.ss.fff)
        if (! (Test-Path $OutFolder -PathType Container) )
        {
            mkdir $OutFolder | Out-Null
        }

    }
    process 
    {
        if ($QueuedPatches.Count -eq 0)
        {
            Write-PsDbDeployLog "    No Patches to Apply"
            return
        }
        try
        {
            $PatchBatches = @()

            AssurePsDbDeploy
            while ($QueuedPatches.Count -gt 0)
            {
                $Patch = $QueuedPatches[0]
                
                NewSqlCommand
                if ($Patch.CheckPoint)
                {
                    if ($PSCmdlet.ShouldProcess($Patch.PatchName,"Checkpoint Patch")) 
                    {
                        # Write-PsDbDeployLog "Checkpoint (mark as executed) - $($Patch.PatchName)"
                        MarkPatchAsExecuted $Patch.PatchName $Patch.Checksum ""
                    }
                }
                else
                {
                    $WhatIfExecute = $true
                    if ($PSCmdlet.ShouldProcess($Patch.PatchName,"Publish Patch")) 
                    {
                        $WhatIfExecute = $false
                    }
                    $Patcher = $Patch.Patcher
                    $dealtWith = $false

                    if ($Patcher.BatchExecution)
                    {
                        if (($PatchBatches.Count -eq 0) -or ($Patcher.PatchType -eq $PatchBatches[0].Patcher.PatchType))
                        {
                            write-verbose "  Postpone Execution for Batch $($Patch.PatchName)"
                            $PatchBatches += $Patch
                        }
                        else
                        {
                            write-verbose "   Executing Batch of $($PatchBatches.count) item(s)"
                            $Patcher.'PerformPatches'($PatchBatches,$WhatIfExecute)
                            $PatchBatches = @()
                                
                            write-vebose "  Postpone Execution for Batch $($Patch.PatchName)"
                            $PatchBatches += $Patch
                        }
                        $dealtWith = $true
                    }
                    else
                    {
                        if ($PatchBatches.Count -gt 0)
                        {
                            write-verbose "   Executing Batch of $($PatchBatches.count) item(s)"
                            CallPatcher $Patcher $PatchBatches $WhatIfExecute
                            $PatchBatches = @()
                        }
                        CallPatcher $Patcher $Patch $WhatIfExecute
                    }
                }
                $QueuedPatches.RemoveAt(0)
            }
            if ($PatchBatches.Count -gt 0)
            {
                CallPatcher $PatchBatches[0].Patcher $PatchBatches $WhatIfExecute
            }
        }
        finally
        {
            $Connection.Close() 
        }
        Catch
        {
            TerminalError $_
        }
    }
}
Export-ModuleMember -Function Publish-Patches

# ----------------------------------------------------------------------------------

function Add-TokenReplacement ($TokenValue, $ReplacementValue)
{
    $TokenReplacements += @{TokenValue=$TokenValue;ReplacementValue=$ReplacementValue}
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

    Write-PsDbDeployLog "Process DB Deployment for $DatabaseName on server $ServerName"
    Write-PsDbDeployLog "    RootFolder: $RootFolderPath"


    if ($SqlLogFile -and (Test-Path $SqlLogFile)) { Remove-Item -Path $SqlLogFile}

    $Script:OutFolderPath = $OutFolderPath
    if (! (Test-Path $OutFolderPath -PathType Container) )
    {
        mkdir $OutFolderPath | Out-Null
    }
    
    Set-DbPatchContext -ServerName $ServerName -DatabaseName $DatabaseName -RootFolderPath $RootFolderPath -Environment $Environment -DisplayCallStack $DisplayCallStack
    $LogSqlOutScreen = $EchoSql
    $SqlLogFile = $SqlLogFile
    $PublishWhatIf = $PublishWhatif

    $QueuedPatches = $QueuedPatches.Clear()
    $Script:TokenReplacements = @()

    AssurePsDbDeploy2

    SetDbVersion -Version $Version -Revision $Revision -Comment $Comment

}

Export-ModuleMember -Function Initialize-PsDbDeploy

Export-ModuleMember -Variable DBPatchContext

Export-ModuleMember -Variable QueuedPatches
