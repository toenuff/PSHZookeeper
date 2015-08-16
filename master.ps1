param(
	  [string] $Name='master1'

)
# dot sourcing functions til this is proper module
. .\module.ps1

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

$timeout = New-Timespan -seconds 1

$watcher = new-object zookeepernet.watcher.countdownwatcher
Register-ObjectEvent -InputObject $watcher -EventName Changed -Action {
	$event.sourceargs |select state, type, path
	$zkclient.exists('/workers/worker1', $watcher) |out-null
} |out-null

$zkclient = new-object ZooKeeperNet.ZooKeeper -ArgumentList @('127.0.0.1:2181,127.0.0.1:2182', $timeout, $watcher)

sleep 1 #need to figure out if we can do asynch creates in .net so sleep would not be necessary

$result = $zkclient |New-EphemeralNode '/master' $Name

write-verbose $result

# grab value without watch
$stat = new-object org.apache.zookeeper.data.stat
#[char[]]$zkclient.getdata('/master', $false, $stat) -join ''

$zkclient |New-MasterDirectoryLayout

$zkclient.exists('/workers/worker1', $watcher) |out-null
