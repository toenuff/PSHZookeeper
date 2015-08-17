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
$GLOBAL:connectionwatcher = $null
$GLOBAL:connectionjob = $null
$GLOBAL:ismaster = $false

function New-MasterDirectoryLayout {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
		 
	)
	$InputObject |New-PersistentDir '/workers'
	$InputObject |New-PersistentDir '/assign'
	$InputObject |New-PersistentDir '/tasks'
	$InputObject |New-PersistentDir '/status'
}

function New-MasterLock {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)
	try {
		$result = $InputObject |New-EphemeralNode '/master' $Name
		$InputObject |Start-Master
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		# if it exists, we need to watch for when it changes so that we can become master
	}
}

function Start-Master {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject
	)

	$InputObject |New-MasterDirectoryLayout

	# grab value without watch
	# $stat = new-object org.apache.zookeeper.data.stat
	# [char[]]$zkclient.getdata('/master', $false, $stat) -join ''


	#$zkclient.exists('/workers/worker1', $watcher) |out-null
}


function New-ConnectionWatcher {
	$GLOBAL:connectionwatcher = new-object zookeepernet.watcher.watcher
	$GLOBAL:connectionjob = Register-ObjectEvent -InputObject $connectionwatcher -EventName Changed -Action {
		$message = $event.sourceargs |select state, type, path
		write-verbose "Connection Watcher Triggered"
		$message
		switch ($message.state[-1]) {
			'SyncConnected' {
				Write-verbose "Starting Master"
				$zkclient
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
	$GLOBAL:connectionjob |receive-job |% {
		write-verbose $_
		if ($_ -eq "RestartZKCLient") {
			$zkclient.dispose()
			$zkclient = $null
			get-job |stop-job -passthru |remove-job
		}
	}
}

