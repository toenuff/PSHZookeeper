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
import-module (join-path $currdir ..\PSHZookeeper.psd1)

$servers = $computername -join ','

# Clean up for dev testing - running repeatedly 
if ($GLOBAL:zkclient) {
    $zkclient.dispose()
    get-job |stop-job -passthru |remove-job
}

function Start-Master {
	# Initialize directories
	$zkclient |New-ZKPersistentNode '/workers'
	$zkclient |New-ZKPersistentNode '/assign'
	$zkclient |New-ZKPersistentNode '/tasks'
	$zkclient |New-ZKPersistentNode '/status'

	New-WorkersWatch
	New-TasksWatch

}

function New-WorkersWatch {
	$workerwatcher = new-object zookeepernet.watcher.watcher 
	$workerwatcherjob = Register-ObjectEvent -InputObject $workerwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Worker Watcher Triggered"
		if ($message.type -eq 'NodeChildrenChanged' -and $message.path -eq '/workers') {
			write-verbose "Detected change"
			$newworkers = $zkclient.Getchildren("/workers", $sender)
			if (!$workers) {
				Write-verbose "Found workers when there was none before"
                Write-verbose ($newworkers -join ', ')
			} else {
				foreach ($worker in $workers) {
					if ($newworkers -notcontains $worker) {
						Write-Verbose "$worker removed from the list of available workers"
                        "/assign/$worker" |Reset-Tasks
					}
				}
			}
			$GLOBAL:workers = $newworkers
		}
	}
	$GLOBAL:workers = $zkclient.GetChildren("/workers", $workerwatcher)
    if ($workers) {
        Write-verbose "Found workers when there was none before"
        Write-verbose ($workers -join ', ')
    }
}

function New-TasksWatch {
	$taskwatcher = new-object zookeepernet.watcher.watcher
	$taskwatcherjob = Register-ObjectEvent -InputObject $taskwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Task Watcher Triggered"
		if ($message.type -eq 'NodeChildrenChanged' -and $message.path -eq '/tasks') {
			write-verbose "Detected change"
			$zkclient.Getchildren("/tasks", $sender) |%{"/tasks/$_"} |start-task
		}
	}
	$zkclient.GetChildren("/tasks", $taskwatcher) |%{"/tasks/$_"} |start-task
}

function GLOBAL:Reset-Tasks {
	param(
		[Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		[string] $path
	)
	PROCESS {
        write-verbose "Resetting all tasks in $path back to /tasks"
        $zkclient.getchildren($path, $false) |% {
            $zkclient |Move-ZKNode "$path/$_" "/tasks"
        }
    }
}

function GLOBAL:Start-Task {
	param(
		[Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		[string] $path
	)
	PROCESS {
		Write-Verbose "processing $path"

        # TODO fix below - probably need to register tasks watcher only when workers exist
        # Either that or put in some sort of /staletasks so that when workers are found for the 
        # first time we reset /staletasks to /tasks
        <#if (!$GLOBAL:workers.count) {
            write-verbose "No workers to schedule tasks on"
            $zkclient |Move-ZKNode $path "/tasks"
        }#>
		$computername = $workers |Get-Random #random scheduler for now
        $zkclient |Move-ZKNode $path "/assign/$computername"
	}
}

function GLOBAL:New-MasterLock {
	try {
		$result = $zkclient |New-ZKEphemeralNode '/master' $Name
		Write-Verbose "Elected Master"
		Start-Master
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		Write-Verbose "Not Elected Master"
		# if it exists, we need to watch for when it changes so that we can become master
		$masterwatcher = new-object zookeepernet.watcher.watcher
		$masterwatcherjob = Register-ObjectEvent -InputObject $masterwatcher -EventName Changed -Action {
			$message = $event.sourceargs |select state, type, path
			write-verbose "Master Watcher Triggered"
			if ($message.type -eq 'NodeDeleted' -and $message.path -eq '/master') {
				New-MasterLock
			} else {
				$message
				throw "Unexpected event in master watcher"
			}
		}
		$zkclient.exists('/master', $masterwatcher) |out-null
	}
}

Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Attempting to grab a lock for master"
    New-MasterLock 
}
