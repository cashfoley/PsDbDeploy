class PatchController
{

	[System.Collections.ArrayList] $QueuedPatches
	[PsDbDatabase] $PsDbDatabase

	PatchController([System.Collections.ArrayList] $QueuedPatches,[PsDbDatabase] $PsDbDatabase)
	{
		$this.QueuedPatches = $QueuedPatches
		$this.PsDbDatabase = $PsDbDatabase
	}

	PublishPatches()
	{
		if ($this.QueuedPatches.Count -eq 0)
        {
            Write-Host "    No Patches to Apply"
            return
        }
        try
        {
            $PatchBatches = @()

            $this.PsDbDatabase.AssurePsDbDeploy()

            while ($this.QueuedPatches.Count -gt 0)
            {
                $Patch = $this.QueuedPatches[0]
                
                $this.PsDbDatabase.AssureSqlCommand()
                if ($Patch.CheckPoint)
                {
                    if ($PSCmdlet.ShouldProcess($Patch.PatchName,"Checkpoint Patch")) 
                    {
                        # Write-Host "Checkpoint (mark as executed) - $($Patch.PatchName)"
                        $this.PatchContext.MarkPatchAsExecuted($Patch.PatchName, $Patch.Checksum, "")
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
                            write-vebose "  Postpone Execution for Batch $($Patch.PatchName)"
                            $PatchBatches += $Patch
                        }
                        else
                        {
                            write-verbose "   Executing Batch of $($PatchBatches.count) item(s)"
                            $Patcher.'PerformPatches'($PatchBatches,$PatchContext,$WhatIfExecute)
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
                            $Patcher.'PerformPatches'($PatchBatches,$PatchContext,$WhatIfExecute)
                            $PatchBatches = @()
                        }
                        $Patcher.'PerformPatches'($Patch,$PatchContext,$WhatIfExecute)
                    }
                }
                $this.QueuedPatches.RemoveAt(0)
            }
            if ($PatchBatches.Count -gt 0)
            {
                $PatchBatches[0].Patcher.'PerformPatches'($PatchBatches,$PatchContext)
            }
        }
        Catch
        {
            $PatchContext.TerminalError($_)
        }
        $PatchContext.Connection.Close()

	}


}