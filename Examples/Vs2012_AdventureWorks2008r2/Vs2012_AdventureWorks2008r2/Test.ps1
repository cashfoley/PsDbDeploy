param ( $ServerName=".", $DatabaseName='Test3')

########################################################################################################################
$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition
$PsDbDeploy = Join-Path $scriptPath "..\..\..\Modules\PsDbDeploy"
Import-Module $PsDbDeploy -force 

########################################################################################################################
Initialize-PsDbDeploy -ServerName $ServerName -DatabaseName $DatabaseName -RootFolderPath $scriptPath 

########################################################################################################################
#  Perform Patches from 'Patches' folder
dir (Join-Path $scriptPath "Patches") -recurse -Filter *.sql `
	| Get-SqlDbPatches -ExecuteOnce -Publish 

########################################################################################################################
#  Perform Patches from 'Functions' folders
$ExcludeFunctions = @(
	'ufnGetAccountingStartDate.sql',
	'ufnLeadingZeros.sql',
	'uspLogError.sql'
)

dir $scriptPath -recurse -Include 'Functions' `
    | %{dir $_ -Filter *.sql | ?{$ExcludeFunctions -notcontains $_.name}} `
	| Get-SqlDbPatches  -Publish `
	-FileContentPattern "CREATE FUNCTION \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
	-BeforeEachPatch @" 
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
    DROP FUNCTION [@(schema)].[@(object)]
"@ `

########################################################################################################################
#  Perform Patches from 'Views' folders
dir $scriptPath -recurse -Include 'Views' | %{dir $_ -Filter *.sql} `
	| Get-SqlDbPatches -Publish `
	-FileContentPattern "CREATE VIEW \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
	-BeforeEachPatch @"
    IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]'))
    DROP VIEW [@(schema)].[@(object)]
"@ 

########################################################################################################################
#  Perform Patches from 'Stored Procedures' folders
dir $scriptPath -recurse -Include 'Stored Procedures' | %{gci $_ -Filter *.sql} `
	| Get-SqlDbPatches -Publish `
		-FileContentPattern "CREATE PROCEDURE \[(?'schema'.*)\]\.\[(?'object'.*?)\]" `
		-BeforeEachPatch @" 
	    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'P', N'PC'))
	    DROP PROCEDURE [@(schema)].[@(object)]
"@ 

