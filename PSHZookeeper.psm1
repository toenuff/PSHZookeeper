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

$zookeeperdll = ([zookeepernet.zookeeper]).assembly.location
$log4netdll = ([log4net.LogManager]).assembly.location

$GLOBAL:zkclient = $null

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

function New-ZKEphemeralNode {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path,
		  [Parameter(Mandatory=$false, Position=1)]
		  [string] $InputText = 0 #defaults to the byte value of zero if there is no data
	)
	write-verbose 'New-ZKEphemeralNode'
	$InputObject.create($path, [byte[]][char[]]$InputText, [ZooKeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::Ephemeral)
}

function Update-ZKNodeData {
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

function New-ZKSequentialNodeData {
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

function New-ZKPersistentNode {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path,
          [Parameter(Mandatory=$false, Position=1)]
          [string] $Data=0
	)
	try {
		Write-Verbose "Create $path"
		$return = $InputObject.create($path, [byte[]][char[]]$data, [ZookeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::PERSISTENT)
		Write-Verbose $return
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		Write-Verbose "$path already exists - skipped"
	}
}

function Remove-ZKNode {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path
	)
    Write-Verbose "Deleting $path"
    $InputObject.Delete($path, $null)
}

function Move-ZKNode {
	param(
        [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
        [Alias('zkclient')]
        [ZookeeperNet.Zookeeper] $InputObject,
		[Parameter(Mandatory=$true, Position=0)]
		[string] $Path,
        [Parameter(Mandatory=$true, Position=1)]
        [string] $Destination
	)
	PROCESS {
		Write-Verbose "Moving $path to $destination"

        Write-Verbose "Getting data from $path"
		$data = $InputObject |Get-ZKData $path 
        write-verbose $data

        $nodename = Split-Path $path -Leaf

	    $InputObject |New-ZKPersistentNode ((join-path $destination $nodename) -replace '\\', '/') $data
        $InputObject |Remove-ZKNode $path 
    }
}

function Get-ZKData {
	param(
		  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
		  [Alias('zkclient')]
		  [ZookeeperNet.Zookeeper] $InputObject,
		  [Parameter(Mandatory=$true, Position=0)]
		  [string] $Path
	)
    [char[]]$InputObject.GetData($path, $false, $null) -join ''
}

function Connect-Zookeeper {
    param(
          [string[]] $Computername = @("127.0.0.1:2181"),
          [System.Timespan] $Timeout = (New-Timespan -Seconds 10),
          [Parameter(Mandatory=$true)]
          [scriptblock] $Action,
          [switch] $Passthru
    )
    # action should return the message "Completed" on a line for it to execute
    # and end.  Otherwise, connect-zookeeper will run in a loop and attempt to reconnect
    # when it is disconnected.

    $servers = $computername -join ','

    $SCRIPT:connectionjob = $null
    $GLOBAL:zkclient = $null

    $finished = $false
    while (!$finished) {
        if (!$GLOBAL:zkclient) {
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
            $GLOBAL:zkclient = new-object ZooKeeperNet.ZooKeeper -ArgumentList @($servers, $timeout, $connectionwatcher)
            while ($SCRIPT.connectionjob.state -eq 'Stopped') {
                sleep 1
            }
        } else {
            if ($passthru) {
                $GLOBAL:zkclient
                break
            }
        }
        $SCRIPT:connectionjob |receive-job -norecurse |% {
            write-verbose $_
            switch ($_) {
                "RestartZKCLient" {
                    $GLOBAL:zkclient.dispose()
                    $GLOBAL:zkclient = $null
                    get-job |stop-job -passthru |remove-job
                    break
                }
                "Completed" {
                    $GLOBAL:zkclient.dispose()
                    $GLOBAL:zkclient = $null
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
