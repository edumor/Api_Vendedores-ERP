$action = New-ScheduledTaskAction `
    -Execute "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe" `
    -Argument "C:\api_vendedores\main.py" `
    -WorkingDirectory "C:\api_vendedores"

$trigger = New-ScheduledTaskTrigger -AtStartup

$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName "ApiVendedoresMicrobell" `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest `
    -Force

Write-Host "Tarea creada. Iniciando ahora..."
Start-ScheduledTask -TaskName "ApiVendedoresMicrobell"
Write-Host "Listo. La API corre en background y arranca automaticamente con Windows."
