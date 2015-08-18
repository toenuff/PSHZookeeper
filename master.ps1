[cmdletbinding()]
param(
	  [string] $Name='master1',
	  [string[]] $Computername = @("127.0.0.1:2181")
)

$servers = $computername -join ','
# dot sourcing functions til this is proper module
. .\module.ps1

# Following is for dev purpose - ensure a blank zkclient
if ($zkclient) {
	$zkclient.dispose()
}
$zkclient = $null
get-job |stop-job -passthru |remove-job

$timeout = New-Timespan -seconds 10
$connectionwatcher = $null
$connectionjob = $null
$workers = $null

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

function start-task {
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

function New-MasterLock {
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

function New-ConnectionWatcher {
	# this function stinks - I'd much wrather create the objects and pass zkclient to it, but zkclient doesn't exist the first time it is created.
	# That's the reason for all of the hoakie code here.  If I tried to use $sender.zkclient it might not exist by the time I need it and I would have no way
	# of waiting for it to show up because it will have already triggered.  The only way around would be to update the c# class to have a default constructor
	# and a connect() method to make the actual connection.
	# I also can't put this in the module code because of scoping issues - I need access to the global zkclient
	$GLOBAL:connectionwatcher = new-object zookeepernet.watcher.watcher
	$GLOBAL:connectionjob = Register-ObjectEvent -InputObject $GLOBAL:connectionwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Connection Watcher Triggered"
		switch ($message.state[-1]) {
			'SyncConnected' {
				Write-verbose "Starting Node"
				$zkclient |New-MasterLock 
				break
			}
			$null {
				break
			}
			default {
				"RestartZKCLient"
			}
		}
	}
}

while ($true) {
	if (!$zkclient) {
		write-verbose "connecting to $servers"
		New-ConnectionWatcher
		$zkclient = new-object ZooKeeperNet.ZooKeeper -ArgumentList @($servers, $timeout, $GLOBAL:connectionwatcher)
	}
	sleep 5
	$GLOBAL:connectionjob |receive-job -norecurse |% {
		write-verbose $_
		if ($_ -eq "RestartZKCLient") {
			$zkclient.dispose()
			$zkclient = $null
			get-job |stop-job -passthru |remove-job
		}
	}
}

