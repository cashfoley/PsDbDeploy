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
Initialize-PsDbDeploy -ServerName $ServerName -DatabaseName $DatabaseName -RootFolderPath $PSScriptRoot -verbose # -SqlLogFile $logfile

#endregion

#region Patches
########################################################################################################################
#  Perform Patches from 'Patches' folder
dir (Join-Path $PSScriptRoot "Patches") -recurse -Filter *.sql `
	| Get-SqlDbPatches -ExecuteOnce -Publish  

#endregion

#  Perform Patches from 'Functions' folders
$ExcludeFunctions = @(
	'ufnGetAccountingStartDate.sql',
	'ufnLeadingZeros.sql',
	'uspLogError.sql'
)

dir $PSScriptRoot -recurse -Include 'Functions' `
    | %{dir $_ -Filter *.sql | ?{$ExcludeFunctions -notcontains $_.name}} `
	| Get-SqlDbPatches -FileContentPatternTemplate Function -Publish 

#  Perform Patches from 'Views' folders
dir $PSScriptRoot -recurse -Include 'Views' | %{dir $_ -Filter *.sql} `
	| Get-SqlDbPatches -FileContentPatternTemplate View -Publish 

#  Perform Patches from 'Stored Procedures' folders
dir $PSScriptRoot -recurse -Include 'Stored Procedures' | %{gci $_ -Filter *.sql} `
	| Get-SqlDbPatches -FileContentPatternTemplate Procedure -Publish 

