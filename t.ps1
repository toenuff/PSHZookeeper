import-module .\zookeeper.psd1 -force -verbose

[ref]$GLOBAL:zkclient = $null

$GLOBAL:payload = 'blah'
Connect-Zookeeper -zkclientref $zkclient -Action {
    Write-verbose "Sending task to Zookeeper"
    $GLOBAL:zkclient |new-sequentialnodedata -path "/tasks/task-" -InputText $payload
    "Completed"
} -verbose
