param ( $ServerName=".", $DatabaseName='Test1')

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition

$PsDbDeploy = Join-Path $scriptPath "..\..\Modules\PsDbDeploy"

Import-Module $PsDbDeploy -force 

Initialize-PsDbDeploy $ServerName $DatabaseName $scriptPath

$BeginTransaction = @"
    SET XACT_ABORT ON
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED
    GO
    BEGIN TRANSACTION;
"@

$EndTransactionScript = @"
    IF @@ERROR <> 0 AND @@TRANCOUNT >  0 ROLLBACK TRANSACTION;
    IF @@TRANCOUNT > 0 COMMIT TRANSACTION;
"@

Publish-Patches `
	-PatchFolderRelativePath 'InitialSetupScripts' `
	-FilePattern  "^.*\.sql$" `
	-BeforEachSQL $BeginTransaction `
	-AfterEachSQL $EndTransactionScript `
    -ExecuteOnce

Publish-Patches `
	-PatchFolderRelativePath 'Patches' `
	-FilePattern  "^.*\.sql$" `
	-BeforEachSQL $BeginTransaction `
	-AfterEachSQL $EndTransactionScript `
    -ExecuteOnce

Publish-Patches `
	-PatchFolderRelativePath 'UserDefinedFunctions\' `
	-FilePattern "(.*\\)?(?'schema'.*)\.(?'object'.*).sql$" `
	-BeforEachSQL @"
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
    DROP FUNCTION [@(schema)].[@(object)]
"@

Publish-Patches `
	-PatchFolderRelativePath 'Views\' `
	-FilePattern "(.*\\)?(?'schema'.*)\.(?'object'.*).sql$" `
	-BeforEachSQL @"
    IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]'))
    DROP VIEW [@(schema)].[@(object)]
"@

Publish-Patches `
	-PatchFolderRelativePath 'StoredProcedures\' `
	-FilePattern "(.*\\)?(?'schema'.*)\.(?'object'.*).sql$" `
	-BeforEachSQL @"
    IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[@(schema)].[@(object)]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [@(schema)].[@(object)]
"@ `
	-ObjectNamePattern "CREATE PROCEDURE \[(?'schema'.*)\]\.\[(?'object'.*)\]" `
	-ObjectNameMatchContent

