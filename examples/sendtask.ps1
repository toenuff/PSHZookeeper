param(
	  [string] $Name='master1',
	  [string[]] $Computername = @("127.0.0.1:2181"),
	  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
	  [PSObject] $InputObject
)
$currdir = ''
if ($MyInvocation.MyCommand.Path) {
    $currdir = Split-Path $MyInvocation.MyCommand.Path
} else {
    $currdir = $pwd -replace '^\S+::',''
}
import-module (join-path $currdir ..\zookeeper.psd1)

$servers = $computername -join ','

$GLOBAL:payload = $inputObject |Convertto-Json

Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Sending task to Zookeeper"
    $zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText $payload
    "Completed"
}
