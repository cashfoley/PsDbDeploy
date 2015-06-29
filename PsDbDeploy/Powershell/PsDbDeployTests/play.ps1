#
# play.ps1
#

"something"

$PsDbDeployModuleFolder = Resolve-Path (Join-Path $PSScriptRoot "..\PsDbDeploy")

. (Join-Path $PsDbDeployModuleFolder "scripts\Database\PsDbDatabase.ps1")
. (Join-Path $PsDbDeployModuleFolder "scripts\Database\SqlServerDatabase.ps1")

$sql = [SqlServerDatabase]::new(".","testdb")

$sql | fl

$sql.ConnectionInfo()
$sql._AssurePsDbDeploy(1)
