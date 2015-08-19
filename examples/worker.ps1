# Copyright 2015 Tome Tanasovski
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

param(
	  [string] $Name='worker1',
	  [string[]] $Computername = @("127.0.0.1:2181")
)

$currdir = ''
if ($MyInvocation.MyCommand.Path) {
    $currdir = Split-Path $MyInvocation.MyCommand.Path
} else {
    $currdir = $pwd -replace '^\S+::',''
}
import-module (join-path $currdir ..\PSHZookeeper.psd1)

$servers = $computername -join ','

# Clean up for dev testing - running repeatedly 
if ($GLOBAL:zkclient) {
    $zkclient.dispose()
    get-job |stop-job -passthru |remove-job
}

$workerdir = join-path $currdir $name
if (!(Test-path $workerdir)) {
	mkdir $workerdir| out-null
}

$GLOBAL:name = $name

function GLOBAL:Start-Task {
	param(
		[Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		[string] $path
	)
	PROCESS {
		Write-Verbose "processing $path"
        $taskid = split-path $path -Leaf
		$data = $zkclient |Get-ZKData $path |ConvertFrom-JSON
        & ([scriptblock]::create($data.cmd)) |out-file -encoding ASCII (join-path $workerdir $taskid)
        $zkclient |Remove-ZKNode $path
	}
}
function New-AssignWatch {
	$watcher = new-object zookeepernet.watcher.watcher
	$watcherjob = Register-ObjectEvent -InputObject $watcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Assign Watcher Triggered"
		if ($message.type -eq 'NodeChildrenChanged' -and $message.path -eq "/assign/$name") {
			write-verbose "Detected change in assignments"
			$zkclient.Getchildren("/assign/$name", $sender) |% {"/assign/$name/$_"} |Start-Task
		}
	}
	$zkclient.GetChildren("/assign/$name", $watcher) |% {"/assign/$name/$_"} |Start-Task
}

function GLOBAL:Start-Worker {
    $zkclient |New-ZKPersistentNode -Path "/assign/$name"
	$zkclient |New-ZKEphemeralNode -Path "/workers/$name"
    New-AssignWatch
}



Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Attempting to grab a lock for master"
    Start-Worker
}
