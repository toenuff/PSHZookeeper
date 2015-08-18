[cmdletbinding()]
param(
	  [string] $Name='master1',
	  [string[]] $Computername = @("127.0.0.1:2181"),
	  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
	  [PSObject] $InputObject
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

$payload = $inputObject |Convertto-Json

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
				Write-verbose "Connection to Zookeeper established"
				Write-verbose "Sending task to Zookeeper"
				$zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText $payload
				"Completed"
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

$finished = $false
while (!$finished) {
	if (!$zkclient) {
		write-verbose "connecting to $servers"
		New-ConnectionWatcher
		$zkclient = new-object ZooKeeperNet.ZooKeeper -ArgumentList @($servers, $timeout, $GLOBAL:connectionwatcher)
	}
	sleep 5
	$GLOBAL:connectionjob |receive-job -norecurse |% {
		write-verbose $_
		switch ($_) {
			"RestartZKCLient" {
				$zkclient.dispose()
				$zkclient = $null
				get-job |stop-job -passthru |remove-job
				break
			}
			"Completed" {
				$zkclient.dispose()
				$zkclient = $null
				$finished=$true
				break
			}
			default {
				$_
			}
		}
	}
}
