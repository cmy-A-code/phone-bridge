# send-media.ps1 - publish a local file to the bridge /media channel (send to phone without WeChat).
# Usage:  send-media.ps1 -Path <local file> [-Name <display name>] [-Json]
# Output: public URL on stdout (put it into the phone reply); exit 0 on success.
# Notes:  bridge must be up on 127.0.0.1:8780. Fully ASCII, no-BOM, hidden-safe.
param(
    [Parameter(Mandatory=$true)][string]$Path,
    [string]$Name = "",
    [switch]$Json
)
$ErrorActionPreference = 'Stop'
try {
    $f = Get-Item -LiteralPath $Path -Force
} catch {
    [Console]::Error.WriteLine("send-media: file not found: $Path")
    exit 1
}
if ($f.PSIsContainer) { [Console]::Error.WriteLine("send-media: is a directory: $Path"); exit 1 }

$body = @{ path = $f.FullName }
if ($Name -ne "") { $body.name = $Name }
$payload = $body | ConvertTo-Json -Compress
$bytes = [System.Text.Encoding]::UTF8.GetBytes($payload)

try {
    $r = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8780/v1/media/put' `
         -ContentType 'application/json; charset=utf-8' -Body $bytes -TimeoutSec 30
} catch {
    [Console]::Error.WriteLine("send-media: bridge call failed: " + $_.Exception.Message)
    exit 2
}
if (-not $r.ok) { [Console]::Error.WriteLine("send-media: bridge returned not-ok"); exit 3 }

# verify locally that the file is actually served
$okLocal = $false
try {
    $resp = Invoke-WebRequest -Method Head -Uri $r.local_url -UseBasicParsing -TimeoutSec 10
    $okLocal = ($resp.StatusCode -eq 200)
} catch { $okLocal = $false }
if (-not $okLocal) { [Console]::Error.WriteLine("send-media: local verify failed"); exit 4 }

if ($Json) {
    $r | ConvertTo-Json -Compress
} elseif ($r.url -ne "") {
    Write-Output $r.url
} else {
    [Console]::Error.WriteLine("send-media: no public url (tunnel down?); local only: " + $r.local_url)
    Write-Output $r.local_url
    exit 5
}
exit 0
