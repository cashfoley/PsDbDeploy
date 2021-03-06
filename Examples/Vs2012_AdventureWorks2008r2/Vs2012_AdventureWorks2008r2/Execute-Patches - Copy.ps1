param ( $ServerName=".", $DatabaseName='Test1')

#region Powershell Init
########################################################################################################################
$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2
#$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition

$PsDbDeploy = Join-Path $PSScriptRoot "..\..\..\Modules\PsDbDeploy"

$logfile = "c:\temp\Patch.sql"

Import-Module $PsDbDeploy -force 
#endregion

#region PsDbDeploy
########################################################################################################################
Initialize-PsDbDeploy -ServerName $ServerName -DatabaseName $DatabaseName -RootFolderPath $PSScriptRoot -verbose -SqlLogFile $logfile 

#endregion

#region Patches
########################################################################################################################
#  Perform Patches from 'Patches' folder
dir (Join-Path $PSScriptRoot "Patches") -recurse -Filter *.sql `
	| Get-SqlDbPatches -ExecuteOnce -Publish  

#endregion

#region Functions
########################################################################################################################
#  Perform Patches from 'Functions' folders
$ExcludeFunctions = @(
	'ufnGetAccountingStartDate.sql',
	'ufnLeadingZeros.sql',
	'uspLogError.sql'
)

dir $PSScriptRoot -recurse -Include 'Functions' `
    | %{dir $_ -Filter *.sql | ?{$ExcludeFunctions -notcontains $_.name}} `
	| Get-SqlDbPatches  -Publish `
	-FileContentPattern "CREATE FUNCTION \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
	-BeforeEachPatch @" 
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
    DROP FUNCTION [@(schema)].[@(object)]
"@ `

#endregion

#region Views
########################################################################################################################
#  Perform Patches from 'Views' folders
dir $PSScriptRoot -recurse -Include 'Views' | %{dir $_ -Filter *.sql} `
	| Get-SqlDbPatches -Publish `
	-FileContentPattern "CREATE VIEW \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
	-BeforeEachPatch @"
    IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]'))
    DROP VIEW [@(schema)].[@(object)]
"@ 

#endregion

#region Stored Procedures
########################################################################################################################
#  Perform Patches from 'Stored Procedures' folders
dir $PSScriptRoot -recurse -Include 'Stored Procedures' | %{gci $_ -Filter *.sql} `
	| Get-SqlDbPatches -Publish `
		-FileContentPattern "CREATE PROCEDURE \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
		-BeforeEachPatch @" 
	    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'P', N'PC'))
	    DROP PROCEDURE [@(schema)].[@(object)]
"@ 
#endregion

