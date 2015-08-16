add-type -path "c:\zookeeper\.net\Zookeeper.Net.3.4.6.2\lib\net40\ZooKeeperNet.dll"

$code = @"
namespace ZooKeeperNet.watcher
{
    using System;
    using System.Runtime.CompilerServices;
    using System.Threading;

	public class CountdownWatcher : IWatcher
	{
		public delegate void ChangedEvent(object sender, WatchedEvent e);
		public event ChangedEvent Changed;

		readonly ManualResetEvent resetEvent = new ManualResetEvent(false);
		private static readonly object sync = new object();

		volatile bool connected;

		public CountdownWatcher()
		{
			Reset();
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Reset()
		{
			resetEvent.Set();
			connected = false;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public virtual void Process(WatchedEvent @event)
		{
			if (@event.State == KeeperState.SyncConnected)
			{
				connected = true;
				lock (sync)
				{
					Monitor.PulseAll(sync);
				}
				resetEvent.Set();
			}
			else
			{
				connected = false;
				lock (sync)
				{
					Monitor.PulseAll(sync);
				}
			}
			if (Changed != null) {
				Changed(this, @event);
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		bool IsConnected()
		{
			return connected;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		void waitForConnected(TimeSpan timeout)
		{
			DateTime expire = DateTime.UtcNow + timeout;
			TimeSpan left = timeout;
			while (!connected && left.TotalMilliseconds > 0)
			{
				lock (sync)
				{
					Monitor.TryEnter(sync, left);
				}
				left = expire - DateTime.UtcNow;
			}
			if (!connected)
			{
				throw new TimeoutException("Did not connect");

			}
		}

		void waitForDisconnected(TimeSpan timeout)
		{
			DateTime expire = DateTime.UtcNow + timeout;
			TimeSpan left = timeout;
			while (connected && left.TotalMilliseconds > 0)
			{
				lock (sync)
				{
					Monitor.TryEnter(sync, left);
				}
				left = expire - DateTime.UtcNow;
			}
			if (connected)
			{
				throw new TimeoutException("Did not disconnect");
			}
		}
	}
}
"@
add-type -path "C:\zookeeper\.net\log4net.1.2.10\lib\2.0\log4net.dll"
add-type -typedefinition $code -referencedassemblies "c:\zookeeper\.net\Zookeeper.Net.3.4.6.2\lib\net40\ZooKeeperNet.dll", "C:\zookeeper\.net\log4net.1.2.10\lib\1.1\log4net.dll"

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
	$zkclient.create($path, [byte[]][char[]]$InputText, [ZooKeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::Ephemeral)
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
	$zkclient.SetData($path, [byte[]][char[]]$InputText, -1)
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
		$return = $zkclient.create($path, 0, [ZookeeperNet.Ids]::OPEN_ACL_UNSAFE, [zookeepernet.createmode]::PERSISTENT)
		Write-Verbose $return
	} catch [ZooKeeperNet.KeeperException+NodeExistsException] {
		Write-Verbose "$path already exists - skipped"
	}
}

