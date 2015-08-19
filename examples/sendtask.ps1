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

param(
	  [string] $Name='master1',
	  [string[]] $Computername = @("127.0.0.1:2181"),
	  [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
	  [PSObject] $InputObject
)
$currdir = ''
if ($MyInvocation.MyCommand.Path) {
    $currdir = Split-Path $MyInvocation.MyCommand.Path
} else {
    $currdir = $pwd -replace '^\S+::',''
}
import-module (join-path $currdir ..\zookeeper.psd1)

$servers = $computername -join ','

$GLOBAL:payload = $inputObject |Convertto-Json

Connect-Zookeeper -ComputerName $servers -Action {
    Write-verbose "Sending task to Zookeeper"
    $zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText $payload
    "Completed"
}
