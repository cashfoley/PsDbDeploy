<#
	My Function
#>


function Get-Function {
    Add-Type -path 'C:\Git\PsDbDeploy2\PsDbDeploy\libraries\PdDbDeploy\bin\Debug\PdDbDeploy.dll' -Verbose

	"My very own!!"

	$c = [PdDbDeploy.Class1]::New()
	$c.TaDa()

}

Export-ModuleMember -Function Get-Function
