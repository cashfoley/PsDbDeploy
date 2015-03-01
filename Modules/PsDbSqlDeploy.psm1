$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2

function ReplaceTokens([string]$str)
{
    foreach ($TokenReplacement in $PatchContext.TokenReplacements)
    {
        $str = $str.Replace($TokenReplacement.TokenValue,$TokenReplacement.ReplacementValue)
    }
    $str
}



$SqlPatcher = New-Module -AsCustomObject -ScriptBlock {
    $PatchType = 'Sql'
    Export-ModuleMember -Variable PatchType

    $BatchExecution = $false
    Export-ModuleMember -Variable BatchExecution

    function PerformPatches
    {
        param
        ( [parameter(Mandatory=$True,ValueFromPipeline=$True,Position=0)]
          $Patches
		, $PatchContext
        , $WhatIfExecute = $True
        )
        process
        {
            foreach ($Patch in $Patches)
            {
                Write-Host $Patch.PatchName
                
                $PatchContext.OutPatchFile($Patch.PatchName, $Patch.patchContent)

                if (!$WhatIfExecute)
                {
                    $PatchContext.NewSqlCommand()
                    New-DbPatch -FilePath $Patch.PatchName -Checksum $Patch.Checksum -Content $Patch.patchContent
                    try
                    {
                        $PatchContext.ExecuteNonQuery( $Patch.patchContent )
                        New-DbExecutionLog -FilePath $Patch.PatchName -Successful
                    }
                    Catch
                    {
                        $PatchContext.ExecuteNonQuery($PatchContext.Constants.RollbackTransactionScript)
                        New-DbExecutionLog -FilePath $Patch.PatchName 
                        throw $_
                    }
                }
            }
        }
    }
    Export-ModuleMember -Function PerformPatches 
}

# ----------------------------------------------------------------------------------
function Add-SqlDbPatches
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]
 
    PARAM
    ( [parameter(Mandatory=$True,ValueFromPipeline=$True,Position=0)]
      [system.IO.FileInfo[]]$PatchFiles
    
    , [string]$BeforeEachPatch
    , [string]$AfterEachPatch
    , [switch]$ExecuteOnce
    , [switch]$CheckPoint
    , [string]$FileNamePattern

    , [ValidateSet("Function","View","Procedure")]
      [string]$FileContentPatternTemplate
    , [string]$FileContentPattern

    , [string]$Comment
    , [switch]$Force
    )
 
    Begin
    {
        $SchemaObjectPattern = "(\s*)(((\[(?'schema'[^\]]*))\])|(?'schema'[^\.[\]]*))\.(((\[(?'object'[^\]]*))\])|(?'object'[^ ]*))"
        switch ($FileContentPatternTemplate)
        {
            "Function" 
            {
                $FileContentPattern = "CREATE\s+FUNCTION$SchemaObjectPattern"
                $BeforeEachPatch = @" 
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
    DROP FUNCTION [@(schema)].[@(object)]
"@
                break;
            }

            "View" 
            {
                $FileContentPattern = "CREATE\s+VIEW$SchemaObjectPattern"
                $BeforeEachPatch = @" 
    IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]'))
    DROP VIEW [@(schema)].[@(object)]
"@
                break;
            }

            "Procedure" 
            {
                $FileContentPattern = "CREATE\s+(PROCEDURE|PROC)$SchemaObjectPattern"
                $BeforeEachPatch = @" 
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [@(schema)].[@(object)]
"@
                break;
            }
        }					
    }
    Process 
    {
        try
        {
            foreach ($PatchFile in $PatchFiles)
            {
                $PatchFile = $PatchFile.Fullname
                Write-Verbose "`$PatchFile: $PatchFile"

                $PatchName = $PatchContext.GetPatchName($PatchFile)
                Write-Verbose "`$PatchName: $PatchName"
            
                if (! ($PatchContext.TestEnvironment($PatchFile) ) )
                {
                    Write-Verbose "`$PatchName ignored because it is the wrong target environment"
                }
                elseif ($QueuedPatches.Where({$_.PatchName -eq $PatchName}))
                {
                    Write-Verbose "`$PatchName ignored because it is already queued"
                }
                else
                {
                    $Checksum = $PatchContext.GetFileChecksum($PatchFile)
                    Write-Verbose "`$Checksum: $Checksum"

                    $PatchCheckSum = [string]($PatchContext.GetChecksumForPatch($PatchName))
            
                    $ApplyPatch = $false
                    if ($Checksum -ne $PatchCheckSum -or $Force)
                    {
                        if ($ExecuteOnce -and ($PatchCheckSum -ne ''))
                        {
                            Write-Warning "Patch $PatchName has changed but will be ignored"
                        }
                        else
                        {
                            $ApplyPatch = $true
                            $BeforeEachPatchStr = ""
                            $AfterEachPatchStr = ""

                            $Patch = $PatchContext.NewPatchObject($SqlPatcher,$PatchFile,$PatchName,$Checksum,$CheckPoint,$Comment,"","")

                            # Annoying use of multiple output
                            # ParseSchemaAndObject verifies match and returns Match Keys.
                            # No keys are a valid result on a match
                            $ObjectKeys = @()
                            if ($FileNamePattern)
                            {
                                Write-Verbose "Evaluate FilenamePattern '$FileNamePattern'"
                                $ApplyPatch, $ObjectKeys = $PatchContext.ParseSchemaAndObject($PatchFile,$FileNamePattern)
                                if (!$ApplyPatch)
                                {
                                    Write-Warning "FileNamePattern does not match patch '$PatchName' - Patch not executed"
                                }
                                else
                                {
                                    $BeforeEachPatchStr = $PatchContext.ReplacePatternValues($BeforeEachPatch, $ObjectKeys)
                                    Write-Verbose "`BeforeEachPatch: $($BeforeEachPatchStr)"
                                    $AfterEachPatchStr = $PatchContext.ReplacePatternValues($AfterEachPatch, $ObjectKeys)
                                    Write-Verbose "`AfterEachPatch: $($AfterEachPatchStr)"
                                }
                            }
                    
                            if ($FileContentPattern -and $ApplyPatch)
                            {
                                Write-Verbose "Evaluate FileContentPattern '$FileContentPattern'"
                                $ApplyPatch, $ObjectKeys = $PatchContext.ParseSchemaAndObject($Patch.PatchContent, $FileContentPattern)
                                if (!$ApplyPatch)
                                {
                                    Write-Warning "FileContentPattern does not match content in patch '$PatchName' - Patch not executed"
                                }
                                else
                                {
                                    $BeforeEachPatchStr = $PatchContext.ReplacePatternValues($BeforeEachPatch, $ObjectKeys)
                                    Write-Verbose "`BeforeEachPatch: $($BeforeEachPatchStr)"
                                    $AfterEachPatchStr = $PatchContext.ReplacePatternValues($AfterEachPatch, $ObjectKeys)
                                    Write-Verbose "`AfterEachPatch: $($AfterEachPatchStr)"
                                }
                            }

                            function GoScript($script)
                            {
                                if ($script)
                                {
                                    $script + "`nGO`n"
                                }
                            }
                            $Patch.PatchContent = (GoScript $PatchContext.Constants.BeginTransctionScript) + 
                                                  (GoScript $BeforeEachPatchStr) + 
                                                  (GoScript (ReplaceTokens $Patch.PatchContent)) + 
                                                  (GoScript $AfterEachPatchStr) + 
                                                  (GoScript $PatchContext.GetMarkPatchAsExecutedString($Patch.PatchName, $Patch.Checksum, "")) +
                                                  (GoScript $PatchContext.Constants.EndTransactionScript)
                        }
                    }
                    else
                    {
                        Write-Verbose "Patch $PatchName current" 
                    }

                    if ($ApplyPatch -or $Force)
                    {
                        [void]$QueuedPatches.Add($Patch) 
                    }
                }
            }
        }
        Catch
        {
            $PatchContext.TerminalError($_)
        }
    }
}

Export-ModuleMember -Function Add-SqlDbPatches
