param(
	  [string] $Name='worker1'
)

# dot sourcing functions til this is proper module
. .\module.ps1

$timeout = New-Timespan -seconds 1

$watcher = new-object zookeepernet.watcher.countdownwatcher
Register-ObjectEvent -InputObject $watcher -EventName Changed -Action {
	$event.sourceargs |select state, type, path
} |out-null

$zkclient = new-object ZooKeeperNet.ZooKeeper -ArgumentList @('127.0.0.1:2181,127.0.0.1:2182', $timeout, $watcher)

sleep 1 #need to figure out if we can do asynch creates in .net so sleep would not be necessary

$result = $zkclient |New-EphemeralNode "/workers/$Name" 'Idle'
write-verbose $result
