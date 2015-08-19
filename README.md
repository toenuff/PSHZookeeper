# PowerShell Zookeeper Cluster for Distributed Computing

I love Zookeeper and I love PowerShell.  Put them together and you have a love fest!  

## What is Zookeeper?
Zookeeper provides a modern way to dynamically add hosts (workers/masters/others) to a cluster or grid of computers.  This compute can be used in a variety of ways.  For example, you can add IIS hosts to a cluster of hosts and have people submit jobs that will be scheduled to run on an available IIS host.  If that IIS host disappears, the system can detect that and automatically configure another host to serve those web pages.  An IIS farm is a simple example of what you can orchestrate around Zookeeper.

## What is PowerShell?
An interpreted language created by Microsoft that is built on and taps into the .NET framework.

## Why?
The intention of this project is to abstract the creation of the code that connects to and watches zookeeper objects into PowerShell module.  It is also a great example of how amazing PowerShell is as a language and it proves that you can write full-fledged applications using the PowerShell interpreter.  Having access to DSC and the automation cmdlets within Windows PowerShell provides a lot of potential for clustering and creating dynamic farms of applications that may otherwise not be possible out of the box.

# Getting Started

This section will walk you through the basics to get Zookeeper running and launch a master-worker-submitter service.

## Start Zookeeper

1. Install Java (Zookeeper is a java app)
1. Download zookeeper from the [Apache Zookeeper Site](https://zookeeper.apache.org/releases.html) and extract it to a directory on your host.
1. `cp conf/zoo_sample.cfg conf/zoo.cfg`
1. Launch zookeeper server with `bin/zkserver.cmd`

## Start the PowerShell Master and Worker
All of the scripts are located in the examples directory of the project.  You can start multiple masters or workers on a single host by running the script multiple times in different powershell runspaces and providing a unique name to each script as a parameter.

To start a master node named master1 run the following:
```powershell
.\examples\master.ps1 master1
```

To start a worker node named worker1 run the following:
```powershell
.\examples\worker.ps1 worker1
```

## Create a task for the worker to take on
```powershell
new-object psobject -property @{
	cmd = "get-process"
} | .\examples\sendtask.ps1
```

Once the task is scheduled it will be picked up by the worker.  The worker creates a directory with its name in the current working directory.  If you inspect that directory, you will see a folder with a task id.  In that folder will be the output of the command that was executed.

# Writing your own

The core of the magic happens in Connect-Zookeeper.  Because Zookeeper is nearly entirely event-driven, it uses a lot of asynchronous activity that is normally complicated and cumbersome.  The abstraction that Connect-Zookeeper provides allows you to save time on writing a lot of code over and over and spend time working on the actual logic your cluster requires.  By default Connect-Zookeeper is run in a tight loop that will attempt to reconnect if there is ever any problems.  The loop can be exited by sending "completed" on a line by itself in the Action scriptblock or you can use -PassThru to exit the loop.  When you use -Passthru your events keep running so that you can develop against a live Zookeeper connection object.  The sample code provides all the details, but here is an example of how to use the 3 modes:

## Tight loop

This code is used by the masters and workers that need to run constantly.  It also illustrates how the $zkclient object may be accessed.  $zkclient may be used to create new events subscriptions or it may be used to perform synchronous actions.

```powershell
Connect-Zookeeper -Computername "127.0.0.1:2181" -Action {
	$zkclient |New-EphemeralNode -Path "/workers/$name"
}
```

## Exiting the loop and closing your connections and event watcher
```powershell
Connect-Zookeeper -Computername "127.0.0.1:2181" -Action {
    $zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText 'some payload'
    "completed"
}
```

## Exiting the loop and keeping your connections and event jobs open
```powershell
Connect-Zookeeper -Computername "127.0.0.1:2181" -Action {
	$zkclient |New-EphemeralNode -Path "/workers/$name"
} -passthru
$zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText 'some payload'
get-job |receive-job # this will show the jobs that are watching events
```

## Variables in the Action block

Besides the special $zkclient variable, you may access any variables that you explicitly put in the global scope of your script. 

```powershell
$GLOBAL:payload = 'This is payload in the global scope'
Connect-Zookeeper -Computername "127.0.0.1:2181" -Action {
    $zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText $GLOBAL:payload
    "completed"
}
```

Global variables can also be set by action blocks.  This makes a great way to have actions provide state to the rest of your actions.  For example, the master node will want to populate a `$GLOBAL:workers` list whenever the `/workers` node gets a new worker.  By the nature of the way Zookeeper works, the event will trigger on a change, you will get a list of all of the children, and then a new watch will be put in place for changes.  This ensures that nothing is lost, but it's very important that you save state in order to figure out what has changed each time an event is triggered.

## Functions in the Action block

In order to make a function available in the action block, you must make the function a global function.  Here is an example of making the New-Something function usable in the Action block:

```powershell
function GLOBAL:New-Something {
    "made something new"
}
Connect-Zookeeper -Computername "127.0.0.1:2181" -Action {
    New-Something
}
```

# Contribute

Feel free to contribute as you see fit.  I'm happy to take pull requests.  <ahem> tests would be very welcome since I did such a good job of creating them to start with :)

# More info and Links

* [Zookeeper](https://zookeeper.apache.org/) - The main project site
* [.NET Zookeeper Dll](https://github.com/ewhauser/zookeeper) - The project and source code
* [Oreilly Zookeeper Book](http://shop.oreilly.com/product/0636920028901.do) - The perfect book to read if you want to know how to zookeeper

* [Tome's Blog](http://powertoe.wordpress.com) - My blog - mostly on Windows PowerShell



