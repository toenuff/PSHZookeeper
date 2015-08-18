$zookeeperdll = ([zookeepernet.zookeeper]).assembly.location
$log4netdll = ([log4net.LogManager]).assembly.location

$code = @"
namespace ZooKeeperNet.watcher
{
    using System;
    using System.Runtime.CompilerServices;
    using System.Threading;

	public class Watcher : IWatcher
	{
		public delegate void ChangedEvent(object sender, WatchedEvent e);
		public event ChangedEvent Changed;

		[MethodImpl(MethodImplOptions.Synchronized)]
		public virtual void Process(WatchedEvent @event)
		{
			if (Changed != null) {
				Changed(this, @event);
			}
		}
	}
}
"@
add-type -typedefinition $code -referencedassemblies $zookeeperdll, $log4netdll

function New-EphemeralNode {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path,
		  [Parameter(Mandatory=$false, Position=1)]
		  [string] $InputText = 0 #defaults to the byte value of zero if there is no data
	)
	write-verbose 'New-EphemeralNode'
	$InputObject.create($path, [byte[]][char[]]$InputText, [ZooKeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::Ephemeral)
}

function Update-NodeData {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path,
		  [Parameter(Mandatory=$false, Position=1)]
		  [string] $InputText = 0 #defaults to the byte value of zero if there is no data
	)
	$InputObject.SetData($path, [byte[]][char[]]$InputText, -1)
}

function New-SequentialNodeData {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path,
		  [Parameter(Mandatory=$false, Position=1)]
		  [string] $InputText = 0 #defaults to the byte value of zero if there is no data
	)
	Write-Verbose "Creating item in $path"
	$return = $InputObject.Create($path, [byte[]][char[]]$InputText, [zookeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::PERSISTENTSequential)
	$return
}

function New-PersistentDir {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path
	)
	try {
		Write-Verbose "Create $path"
		$return = $InputObject.create($path, 0, [ZookeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::PERSISTENT)
		Write-Verbose $return
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		Write-Verbose "$path already exists - skipped"
	}
}

function Connect-Zookeeper {
    param(
          [string[]] $Computername = @("127.0.0.1:2181"),
          [System.Timespan] $Timeout = (New-Timespan -Seconds 10),
          [Parameter(Mandatory=$true)]
          [ref]$zkclientref,
          [Parameter(Mandatory=$true)]
          [scriptblock] $Action
          
    )
    # action should return the message "Completed" on a line for it to execute
    # and end.  Otherwise, connect-zookeeper will run in a loop and attempt to reconnect
    # when it is disconnected.

    $servers = $computername -join ','

    $SCRIPT:connectionjob = $null
    $zkclientref.value = $null

    $finished = $false
    while (!$finished) {
        if (!$zkclientref.value) {
            write-verbose "connecting to $servers"
            $connectionwatcher = new-object zookeepernet.watcher.watcher
            $actionstart = @'
    $message = $event.sourceargs |select state, type, path
    write-verbose "Connection Watcher Triggered"
    switch ($message.state[-1]) {
        'SyncConnected' {
            Write-verbose "Connection to Zookeeper established"

'@
            $actionend = @'

            break
        }
        $null {
            break
        }
        default {
            "RestartZKCLient"
        }
    }
'@
            $scriptblock = [scriptblock]::create($actionstart + $action.tostring() + $actionend)
            $SCRIPT:connectionjob = Register-ObjectEvent -InputObject $connectionwatcher -EventName Changed -Action $scriptblock
            $zkclientref.value = new-object ZooKeeperNet.ZooKeeper -ArgumentList @($servers, $timeout, $connectionwatcher)
        }
        sleep 5
        $SCRIPT:connectionjob |receive-job -norecurse |% {
            write-verbose $_
            switch ($_) {
                "RestartZKCLient" {
                    $zkclientref.value.dispose()
                    $zkclientref.value = $null
                    get-job |stop-job -passthru |remove-job
                    break
                }
                "Completed" {
                    $zkclientref.value.dispose()
                    $zkclientref.value = $null
                    $finished = $true
                    $SCRIPT:connectionjob |stop-job -passthru |remove-job
                    break
                }
                default {
                    $_
                }
            }
        }
    }
}
