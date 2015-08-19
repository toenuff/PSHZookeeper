[cmdletbinding()]
param(
	  [string] $Name='master1',
	  [string[]] $Computername = @("127.0.0.1:2181")
)

$currdir = ''
if ($MyInvocation.MyCommand.Path) {
    $currdir = Split-Path $MyInvocation.MyCommand.Path
} else {
    $currdir = $pwd -replace '^\S+::',''
}
import-module (join-path $currdir ..\zookeeper.psd1)

$servers = $computername -join ','

$workerdir = join-path $currdir $name
if (!(Test-path $workerdir)) {
	mkdir $workerdir| out-null
}


function GLOBAL:Start-Worker {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)
	$InputObject |New-EphemeralNode -Path "/workers/$name"
}

Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Attempting to grab a lock for master"
    $GLOBAL:zkclient |Start-Worker
}
