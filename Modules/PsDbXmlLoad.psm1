$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2


$XmlLoadPatcher = New-Module -AsCustomObject -ScriptBlock {
    $PatchType = 'XmlLoad'
    Export-ModuleMember -Variable PatchType

    $BatchExecution = $true
    Export-ModuleMember -Variable BatchExecution


#region Data Load Queries
# ----------------------------------------------------------------------------------------------
# Query to get all the FKs going to the Target Table
$FksToTableQuery = @"
SELECT QUOTENAME(ctu.Table_Schema) Table_Schema
     , QUOTENAME(ctu.Table_Name) Table_Name
     , QUOTENAME(rc.Constraint_Name) Constraint_Name
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
       JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
         ON tc.Constraint_Name = rc.Unique_Constraint_Name AND tc.Constraint_Schema = rc.Constraint_Schema
       JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu
         ON rc.Constraint_Name = ctu.Constraint_Name AND rc.Constraint_Schema = ctu.Constraint_Schema
 WHERE tc.table_schema = '{0}'
   AND tc.table_name = '{1}'
"@

# ----------------------------------------------------------------------------------------------
# Query to get Table Definition from Info Schema
$TableDefinitionSQL = @"
SELECT Column_Name
     , Data_Type
     , Character_Maximum_Length
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE Table_Schema = N'{0}'
   AND Table_Name = N'{1}'
 ORDER BY Ordinal_Position
"@

# ----------------------------------------------------------------------------------------------
# Scalar Query to determine if Table has Identity Column
$TableHasIdentity = @"
	SELECT Count(*)
	  FROM sys.tables AS t
	  JOIN sys.identity_columns ic ON t.object_id = ic.object_id
	 WHERE t.name = N'{1}'
	   AND SCHEMA_NAME(schema_id) = N'{0}'
"@

# ----------------------------------------------------------------------------------------------
# SQL to load a prepared XML document.  It is then accessed through OPENXML
$LoadXmlDocumentSQL = @"
DECLARE @XmlDocument nvarchar(max)
SET @XmlDocument = N'<ROOT>
{0}
</ROOT>'

DECLARE @DocHandle  int
EXEC sp_xml_preparedocument @DocHandle OUTPUT, @XmlDocument
"@

# ----------------------------------------------------------------------------------------------
# SQL to insert Prepared OPENXML document into a Table
$InsertXmlDataSQL = @"
PRINT N'INSERT XML Data INTO [{0}].[{1}]'
INSERT 
  INTO [{0}].[{1}]
     ( {2} 
     )
SELECT {2}
  FROM OPENXML (@DocHandle, '/ROOT/{0}.{1}',1) 
  WITH 
     ( {3}
     )

EXEC sp_xml_removedocument @DocHandle

"@

#endregion

# ----------------------------------------------------------------------------------------------
# Generates SQL to Disable or Enable FKs to all DataSetTables
function Get-FKSql($Patches, [switch]$Disable)
{
    if ($Disable)
    {
        $CheckStr = "NOCHECK"
        $ActionMessages = "Disable","for"
    }
    else
    {
        $CheckStr = "WITH CHECK CHECK"
        $ActionMessages = "Enable","After"
    }

    "PRINT N'################################################################################'"
    "PRINT N'   $($ActionMessages[0]) FK Constraints $($ActionMessages[1]) Datatable Load'"
    "PRINT N'################################################################################'`n"
    foreach ($Patch in $Patches)
    {
        $SqlCommand.CommandText = $FksToTableQuery -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName
        "PRINT N'------------------------------------------------'"
        "`nPRINT N'$($ActionMessages[0]) FKs to {0}.{1}'" -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName
        $sqlReader = $SqlCommand.ExecuteReader()
        try
        {
            while ($sqlReader.Read()) 
            { 
                $TargetTableSchema = $sqlReader["Table_Schema"]
                $TargetTableName = $sqlReader["Table_Name"]
                $TargetTableConstraintName = $sqlReader["Constraint_Name"]
		        "PRINT N'{3} FK {2} on {0}.{1}'" -f $TargetTableSchema,$TargetTableName,$TargetTableConstraintName, $($ActionMessages[0])

		        "ALTER TABLE {0}.{1} {3} CONSTRAINT {2}`nGO`n" -f $TargetTableSchema,$TargetTableName,$TargetTableConstraintName, $CheckStr
            }
        }
        finally
        {
            $sqlReader.Close()
        }
    }
}

# ----------------------------------------------------------------------------------------------
function get-XmlInsertSql($Patches,[switch]$IncludTimestamps)
{
    "PRINT N'################################################################################'"
    "PRINT N'   Table Data Load'"
    "PRINT N'################################################################################'`n"
    foreach ($Patch in $Patches)
    {
        $TableFullName = "[{0}].[{1}]" -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName
        $ColumnNames = @()
        $ColumnDataTypes = @()
        $SqlCommand.CommandText = $TableDefinitionSQL -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName
        $sqlReader = $SqlCommand.ExecuteReader()
        try
        {
            while ($sqlReader.Read()) 
            { 
                if ($IncludTimestamps -or ($sqlReader["Data_Type"] -ne 'TIMESTAMP'))
                {
                    $ColumnName = "[" + $sqlReader["Column_Name"] +"]"
                    $ColumnDateType = $sqlReader["Data_Type"]
                    $ColumnCharacterMaximumLength = $sqlReader["Character_Maximum_Length"]
                    
                    $ColumnNames += $ColumnName

                    if ($ColumnCharacterMaximumLength -is [System.DBNull]) { $ColumnDataLength = ''}
                    elseif ($ColumnCharacterMaximumLength -eq -1) {$ColumnDataLength = '(MAX)'}
                    else {$ColumnDataLength = "($ColumnCharacterMaximumLength)"}

                    $ColumnDataTypes += "{0} {1}{2}" -f $ColumnName,$ColumnDateType,$ColumnDataLength
                }
            }
        }
        finally
        {
            $sqlReader.Close()
        }
        "SET NOCOUNT ON`n"

        "PRINT N'DELETE all rows FROM $TableFullName'"
        "DELETE FROM $TableFullName`n"

        $LoadXmlDocumentSQL -f $Patch.PatchContent.trim().Replace("'","''")

        $SqlCommand.CommandText = $TableHasIdentity -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName
        $HasIdentity = $SqlCommand.ExecuteScalar()
        if ($HasIdentity -gt 0)
        {
            "SET IDENTITY_INSERT $TableFullName ON`n"
        }

        $InsertColumnNames = $ColumnNames -join "`n     , "
        $InsertColumnDefs  = $ColumnDataTypes -join "`n     , "
        $InsertXmlDataSQL -f $Patch.PatchAttributes.SchemaName,$Patch.PatchAttributes.TableName, $InsertColumnNames, $InsertColumnDefs

        if ($HasIdentity -gt 0)
        {
            "SET IDENTITY_INSERT $TableFullName OFF"
        }
        "GO`n"
    }
}


    function PerformPatches
    {
        param
        ( [parameter(Mandatory=$True,ValueFromPipeline=$True,Position=0)]
            $Patches
        )

        NewSqlCommand 
        try
        {
            function GoScript($script)
            {
                if ($script)
                {
                    $script + "`nGO`n"
                }
            }

            $DataSqlBuilder = New-Object System.Text.StringBuilder
            function appendQuery($sql)
            {
                foreach ($line in $sql)
                {
                    $DataSqlBuilder.AppendLine($line) | Out-Null
                }
            }

            appendQuery (GoScript $Constants.BeginTransctionScript)
            appendQuery (GoScript (Get-FKSql $Patches -Disable))
            appendQuery (GoScript (get-XmlInsertSql $Patches))
            appendQuery (GoScript (Get-FKSql $Patches))

            
            foreach($Patch in $Patches)
            {
                appendQuery (GoScript (GetMarkPatchAsExecutedString $Patch.PatchName $Patch.Checksum ""))
            }
            appendQuery (GoScript ($Constants.EndTransactionScript))

            ExecuteNonQuery $DataSqlBuilder.ToString()
        }
        Catch
        {
            ExecuteNonQuery $Constants.RollbackTransactionScript
            throw $_
        }
    }

    Export-ModuleMember -Function PerformPatches 
}

# ----------------------------------------------------------------------------------
function Add-XmlDbPatches
{
    [CmdletBinding(
        SupportsShouldProcess=$True,ConfirmImpact=’Medium’
    )]
 
    PARAM
    ( [parameter(Mandatory=$True,ValueFromPipeline=$True,Position=0)]
      [system.IO.FileInfo[]]$PatchFiles
    , [switch]$ExecuteOnce
    , [switch]$CheckPoint
    , [string]$FileNamePattern
    , [string]$Comment
    , [switch]$Force
    )
 
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
            
                $Checksum = GetFileChecksum $PatchFile
                Write-Verbose "`$Checksum: $Checksum"

                $PatchCheckSum = [string](GetChecksumForPatch $PatchName)
            
                $Patch = NewPatchObject $XmlLoadPatcher $PatchFile $PatchName $Checksum $CheckPoint
                $ApplyPatch = $false
                if ($Checkpoint)
                {
                    $ApplyPatch = $true
                }
                elseif ($Checksum -ne $PatchCheckSum -or $Force)
                {
                    if ($ExecuteOnce -and ($PatchCheckSum -ne ''))
                    {
                        Write-Warning "Patch $PatchName has changed but will be ignored"
                    }
                    else
                    {
                        $filename = Split-Path $PatchName -Leaf

                        if (!($filename -match "^(?'schema'.*)\.(?'table'.*)\.xml$"))
                        {
                            Write-Warning "File Name Pattern <schema>.<table>.xml does not match patch '$PatchName' - Patch not executed"
                        }
                        else
                        {
                            $Patch.PatchAttributes.SchemaName = $matches.schema;
                            $Patch.PatchAttributes.TableName = $matches.table;
                            $ApplyPatch = $true
                        }

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
        Catch
        {
            TerminalError $_
        }
    }
}

Export-ModuleMember -Function Add-XmlDbPatches
