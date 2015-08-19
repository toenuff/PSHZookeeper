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


function Start-Master {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
		 
	)
	# Initialize directories
	$InputObject |New-PersistentDir '/workers'
	$InputObject |New-PersistentDir '/assign'
	$InputObject |New-PersistentDir '/tasks'
	$InputObject |New-PersistentDir '/status'

	$InputObject |New-WorkersWatch
	$InputObject |New-TasksWatch

}

function New-WorkersWatch {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)
	$workerwatcher = new-object zookeepernet.watcher.watcher |add-member -NotePropertyName zkclient -NotePropertyValue $InputObject -passthru
	$workerwatcher |add-member -notepropertyname workers -notepropertyvalue [ref]$GLOBAL:workers
	$workerwatcherjob = Register-ObjectEvent -InputObject $workerwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Worker Watcher Triggered"
		if ($message.type -eq 'NodeChildrenChanged' -and $message.path -eq '/workers') {
			write-verbose "Detected change"
			$newworkers = $sender.zkclient.Getchildren("/workers", $sender)
			if (!$GLOBAL:workers) {
				Write-verbose "Found workers when there was none before"
			} else {
				foreach ($worker in $workers) {
					if ($newworkers -notcontains $worker) {
						Write-Verbose "$worker removed from the list of available workers"
						#TODO Need logic to remove task from the workers tasks and reassign
					}
				}
			}
			$GLOBAL:workers = $newworkers
		}
	}
	$GLOBAL:workers = $InputObject.GetChildren("/workers", $workerwatcher)
}

function New-TasksWatch {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)
	$taskwatcher = new-object zookeepernet.watcher.watcher |add-member -NotePropertyName zkclient -NotePropertyValue $InputObject -passthru
	$taskwatcherjob = Register-ObjectEvent -InputObject $taskwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Task Watcher Triggered"
		if ($message.type -eq 'NodeChildrenChanged' -and $message.path -eq '/tasks') {
			write-verbose "Detected change"
			$sender.zkclient.Getchildren("/tasks", $sender) |start-task
		}
	}
	$InputObject.GetChildren("/tasks", $taskwatcher) |start-task
}

function Start-Task {
	param(
		[Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		[string[]] $path
	)
	PROCESS {
		write-verbose "processing $path"
		$computername = $workers |get-random #random scheduler for now
		$path
	}
}

function GLOBAL:New-MasterLock {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)
	try {
		$result = $InputObject |New-EphemeralNode '/master' $Name
		Write-Verbose "Elected Master"
		$InputObject |Start-Master
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		Write-Verbose "Not Elected Master"
		# if it exists, we need to watch for when it changes so that we can become master
		$masterwatcher = new-object zookeepernet.watcher.watcher |add-member -NotePropertyName zkclient -NotePropertyValue $zkclient -passthru
		$masterwatcherjob = Register-ObjectEvent -InputObject $masterwatcher -EventName Changed -Action {
			$message = $event.sourceargs |select state, type, path
			write-verbose "Master Watcher Triggered"
			if ($message.type -eq 'NodeDeleted' -and $message.path -eq '/master') {
				$sender.zkclient |New-MasterLock
			} else {
				$message
				throw "Unexpected event in master watcher"
			}
		}
		$InputObject.exists('/master', $masterwatcher) |out-null
	}
}

Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Attempting to grab a lock for master"
    $GLOBAL:zkclient |New-MasterLock 
}
