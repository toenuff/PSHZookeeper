$zookeeperdll = "c:\zookeeper\.net\Zookeeper.Net.3.4.6.2\lib\net40\ZooKeeperNet.dll"
$log4netdll = "C:\zookeeper\.net\log4net.1.2.10\lib\2.0\log4net.dll"
add-type -path $zookeeperdll
add-type -path $log4netdll

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

