# Create or update a .env file from .env.example
# Usage: run this script from the repository root in PowerShell

$example = Join-Path -Path (Get-Location) -ChildPath ".env.example"
$target = Join-Path -Path (Get-Location) -ChildPath ".env"

if (-not (Test-Path $example)) {
    Write-Error ".env.example not found in the current directory."
    exit 1
}

if (Test-Path $target) {
    Write-Host ".env already exists. Do you want to overwrite it? (Y/N)" -NoNewline
    $resp = Read-Host
    if ($resp -notin @('Y','y')) {
        Write-Host "Aborting. .env left unchanged."
        exit 0
    }
}

Copy-Item -Path $example -Destination $target -Force
Write-Host "Copied .env.example to .env"

# Open .env in Notepad for editing
try {
    Start-Process notepad.exe $target
} catch {
    Write-Host "Could not open Notepad. Please edit $target manually." 
}

Write-Host "Edit the .env file and set your values (TWITTER_BEARER_TOKEN, etc.)."
