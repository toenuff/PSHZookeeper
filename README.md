# PowerShell Zookeeper Cluster for Distributed Computing

I love Zookeeper and I love PowerShell.  Put them together and you have a love fest!  

## What is Zookeeper?
Zookeeper provides a modern way to dynamically add hosts (workers/masters/others) to a cluster or grid of computers.  This compute can be used in a variety of ways.  For example, you can add IIS hosts to a cluster of hosts and have people submit jobs that will be scheduled to run on an available IIS host.  If that IIS host disappears, the system can detect that and automatically configure another host to serve those web pages.  An IIS farm is a simple example of what you can orchestrate around Zookeeper.

## What is PowerShell?
An interpreted language created by Microsoft that is built on and taps into the .NET framework.

## Why?
The intention of this project is to share my starting point with creating a complete zookeeper application built on PowerShell.  It will illustrate the core components to creating a master-worker-submitter workflow and provide a way to write a distributed grid-like compute cluster that can execute powershell code.  It is not intended for production, but it can be used as a starting point to create a production app.  There are plenty of bits that can be fixed and made cleaner, but in an effort to share and get people excited by the idea, I'm putting it out early.

# Getting Started

## Start Zookeeper

1. Download zookeeper from the [Apache Zookeeper Site](https://zookeeper.apache.org/releases.html) and extract it to a directory on your host.
1. `cp conf/zoo_sample.cfg conf/zoo.cfg`
1. Launch zookeeper server with `bin/zkserver.cmd`

## Start the PowerShell Master and Worker
You can start multiple masters or workers on a single host by running the script multiple times in different powershell runspaces and providing a unique name to each script as a parameter.  It is extremely important that you launch a powershell window and dot (.) source each script.  The current iteration only works this way.

To start a master node named master1 run the following:
```powershell
. .\master.ps1 master1
```

To start a worker node named worker1 run the following:
```powershell
. .\worker.ps1 worker1
```

## Create a task for the worker to take on
```powershell
new-object psobject -property @{
	command = "get-process"
} |. .\sendtask.ps1
```

Once the task is scheduled it will be picked up by the worker.  The worker creates a directory with its name in the current working directory.  If you inspect that directory, you will see a folder with a task id.  In that folder will be the output of the command that was executed.


