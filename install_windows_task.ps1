param(
    [int]$IntervalMinutes = 30,
    [string]$TaskName = "ERA5-Pakistan-Scheduler"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunScript = Join-Path $ProjectRoot "run_tick.ps1"

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$RunScript`" run" `
    -WorkingDirectory $ProjectRoot

$Trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Polls (after 1.5h CDS processing delay), transfers to Google Drive, and submits non-overlapping ERA5 Pakistan pressure-level (37 levels) & MARS model-level (137 levels) requests." `
    -Force | Out-Null

Write-Host "Registered Windows Scheduled Task '$TaskName' (every $IntervalMinutes minutes)."
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
