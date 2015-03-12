$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2

function ReplaceTokens([string]$str)
{
    foreach ($TokenReplacement in $TokenReplacements)
    {
        $str = $str.Replace($TokenReplacement.TokenValue,$TokenReplacement.ReplacementValue)
    }
    $str
}

function Perform-SqlPatches
{
    param
    ( [parameter(Mandatory=$True,ValueFromPipeline=$True,Position=0)]
        $Patches
    , $WhatIfExecute = $True
    )
    process
    {
        foreach ($Patch in $Patches)
        {
            #Write-PsDbDeployLog $Patch.PatchName
            Write-Verbose $Patch.PatchName
                
            OutPatchFile $Patch.PatchName $Patch.patchContent

            if (!$WhatIfExecute)
            {
                NewSqlCommand 
                New-DbPatch -FilePath $Patch.PatchName -Checksum $Patch.Checksum -Content $Patch.patchContent
                try
                {
                    ExecuteNonQuery  $Patch.patchContent 
                    New-DbExecutionLog -FilePath $Patch.PatchName -Successful
                }
                Catch
                {
                    ExecuteNonQuery $Constants.RollbackTransactionScript
                    New-DbExecutionLog -FilePath $Patch.PatchName 
                    throw $_
                }
            }
        }
    }
}

$SqlPatcher = New-Object -TypeName PSObject -Property (@{PatchType = 'Sql';BatchExecution = $false})

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

                $PatchName = GetPatchName $PatchFile
                Write-Verbose "`$PatchName: $PatchName"
            
                if (! (TestEnvironment $PatchFile) ) 
                {
                    Write-Verbose "`$PatchName ignored because it is the wrong target environment"
                }
                elseif ($QueuedPatches.Where({$_.PatchName -eq $PatchName}))
                {
                    Write-Verbose "`$PatchName ignored because it is already queued"
                }
                else
                {
                    $Patch = NewPatchObject $SqlPatcher $PatchFile $PatchName $CheckPoint $Comment $false $ExecuteOnce $Force

                    Write-Verbose "`$Checksum: $Patch.Checksum"

                    $PatchCheckSum = [string](GetChecksumForPatch $PatchName)
            
                    $ApplyPatch = $false
                    if ($Patch.Force)
                    {
                        Write-Verbose "Force Execution '$PatchName'" 
                        $ApplyPatch=$true
                    }
                    elseif ($Patch.Checksum -ne $PatchCheckSum)
                    {
                        if ($Patch.Ignore)
                        {
                            Write-Verbose "Ignoring $PatchName"
                        }
                        elseif ($Patch.ExecuteOnce -and ($PatchCheckSum -ne ''))
                        {
                            Write-Warning "Patch $PatchName has changed but will be ignored"
                        }
                        else
                        {
                            $ApplyPatch = $true
                        }
                    }
                    else
                    {
                        Write-Verbose "Patch $PatchName is up to date"
                    }

                    if ($ApplyPatch)
                    {
                        $ApplyPatch = $true
                        $BeforeEachPatchStr = ""
                        $AfterEachPatchStr = ""


                        # Annoying use of multiple output
                        # ParseSchemaAndObject verifies match and returns Match Keys.
                        # No keys are a valid result on a match
                        $ObjectKeys = @()
                        if ($FileNamePattern)
                        {
                            Write-Verbose "Evaluate FilenamePattern '$FileNamePattern'"
                            $ApplyPatch, $ObjectKeys = ParseSchemaAndObject $PatchFile $FileNamePattern
                            if (!$ApplyPatch)
                            {
                                Write-Warning "FileNamePattern does not match patch '$PatchName' - Patch not executed"
                            }
                            else
                            {
                                $BeforeEachPatchStr = ReplacePatternValues $BeforeEachPatch $ObjectKeys
                                Write-Verbose "`BeforeEachPatch: $($BeforeEachPatchStr)"
                                $AfterEachPatchStr = ReplacePatternValues $AfterEachPatch $ObjectKeys
                                Write-Verbose "`AfterEachPatch: $($AfterEachPatchStr)"
                            }
                        }
                    
                        if ($FileContentPattern -and $ApplyPatch)
                        {
                            Write-Verbose "Evaluate FileContentPattern '$FileContentPattern'"
                            $ApplyPatch, $ObjectKeys = ParseSchemaAndObject $Patch.PatchContent $FileContentPattern
                            if (!$ApplyPatch)
                            {
                                Write-Warning "FileContentPattern does not match content in patch '$PatchName' - Patch not executed"
                            }
                            else
                            {
                                $BeforeEachPatchStr = ReplacePatternValues $BeforeEachPatch $ObjectKeys
                                Write-Verbose "`BeforeEachPatch: $($BeforeEachPatchStr)"
                                $AfterEachPatchStr = ReplacePatternValues $AfterEachPatch $ObjectKeys
                                Write-Verbose "`AfterEachPatch: $($AfterEachPatchStr)"
                            }
                        }

                        $ScriptBuilder = New-Object System.Text.StringBuilder
                        function GoScript($script)
                        {
                            if ($script)
                            {
                                $ScriptBuilder.Append($script) | Out-Null
                                $ScriptBuilder.Append("`nGO`n") | Out-Null
                            }
                        }

                        if (! $Patch.NoTransaction)
                        {
                            GoScript $Constants.BeginTransctionScript
                        }

                        GoScript $BeforeEachPatchStr 
                        GoScript (ReplaceTokens $Patch.PatchContent) 
                        GoScript $AfterEachPatchStr 
                        GoScript (GetMarkPatchAsExecutedString $Patch.PatchName $Patch.Checksum "")

                        if (! $Patch.NoTransaction)
                        {
                            GoScript $Constants.EndTransactionScript
                        }
                                            
                        $Patch.PatchContent = $ScriptBuilder.ToString()                        
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
            TerminalError $_
        }
    }
}

Export-ModuleMember -Function Add-SqlDbPatches
