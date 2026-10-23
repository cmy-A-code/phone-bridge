param(
  [Parameter(Mandatory=$true)][string]$TaskFile,
  [Parameter(Mandatory=$true)][string]$TaskId,
  [string]$ConvName = ""
)
$ErrorActionPreference = 'Continue'
$root  = Split-Path -Parent $MyInvocation.MyCommand.Path
$tools = Split-Path -Parent $root
$gx    = Join-Path $tools 'gx.exe'
$log   = Join-Path $root 'inject.log'

# calibrated physical-pixel targets for 2560x1600@150% (window at (1293,183)-(2436,1074))
$NEWCHAT_X = 1441; $NEWCHAT_Y = 730
$INPUT_X   = 1700; $INPUT_Y   = 878

function L([string]$m) { try { "$(Get-Date -Format 'HH:mm:ss.fff') [inject] $m" | Add-Content -Path $log -Encoding UTF8 } catch { } }
function RunPS([string[]]$a) { $script:LastRC = 0; $out = & powershell -NoProfile -ExecutionPolicy Bypass @a; $script:LastRC = $LASTEXITCODE; return $out }

L "START task=$TaskId file=$TaskFile"
if (-not (Test-Path $TaskFile)) { L "TaskFile missing"; exit 13 }

# 1) preflight: start guard + announce + badge
$pfOut = RunPS @('-File', (Join-Path $tools 'preflight.ps1'), '-Tag', 'bridge-task', '-Minutes', '30')
$pfRc = $script:LastRC
L "preflight rc=$pfRc"
if ($pfRc -ne 0) { L ("preflight FAILED: " + (($pfOut | Out-String).Trim())); exit 10 }

# 2) focus Chatbox (through the gate)
$fo = RunPS @('-File', (Join-Path $tools 'invoke-guarded.ps1'), '-Command', "& '$(Join-Path $tools 'focus_window.ps1')' -Match 'Chatbox'", '-Tag', 'bridge-focus')
$frc = $script:LastRC
$fs = ($fo | Out-String).Trim()
L "focus rc=$frc out=$fs"
if ($frc -ne 0 -or $fs -notmatch 'fgIsTarget=True') {
  L "focus FAILED - aborting without clicking"
  RunPS @('-File', (Join-Path $tools 'end-session.ps1')) | Out-Null
  exit 11
}

# 2b) geometry guard: both click targets must be inside the Chatbox window (physical px)
Add-Type @'
using System; using System.Runtime.InteropServices;
public class BRect {
  [DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int Left, Top, Right, Bottom; }
}
'@
[void][BRect]::SetProcessDPIAware()
$cbWin = Get-Process Chatbox -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1
$geoOk = $false
if ($cbWin) {
  $wr = New-Object BRect+RECT
  if ([BRect]::GetWindowRect($cbWin.MainWindowHandle, [ref]$wr)) {
    $geoOk = ($NEWCHAT_X -ge $wr.Left -and $NEWCHAT_X -le $wr.Right -and $NEWCHAT_Y -ge $wr.Top -and $NEWCHAT_Y -le $wr.Bottom -and
              $INPUT_X -ge $wr.Left -and $INPUT_X -le $wr.Right -and $INPUT_Y -ge $wr.Top -and $INPUT_Y -le $wr.Bottom)
    L "chatbox rect=($($wr.Left),$($wr.Top))-($($wr.Right),$($wr.Bottom)) targetsInside=$geoOk"
  }
}
if (-not $geoOk) { L "geometry guard FAILED - abort"; RunPS @('-File', (Join-Path $tools 'end-session.ps1')) | Out-Null; exit 14 }

# 3) open a fresh conversation and click its input box
& $gx @("click:$NEWCHAT_X,$NEWCHAT_Y",'wait:900') | Out-Null
$rcA = $LASTEXITCODE
L "gx newchat rc=$rcA"
if ($rcA -ne 0) { RunPS @('-File', (Join-Path $tools 'end-session.ps1')) | Out-Null; exit 12 }

& $gx @("click:$INPUT_X,$INPUT_Y",'wait:300') | Out-Null
$rcB = $LASTEXITCODE
L "gx input rc=$rcB"
if ($rcB -ne 0) { RunPS @('-File', (Join-Path $tools 'end-session.ps1')) | Out-Null; exit 12 }

# 4) paste the task text from clipboard
Set-Clipboard -Value (Get-Content -Raw -Encoding UTF8 $TaskFile)
Start-Sleep -Milliseconds 250
$po = RunPS @('-File', (Join-Path $tools 'gcombo.ps1'), '-Combo', 'ctrl+v', '-Tag', 'bridge-paste')
$prc = $script:LastRC
L "paste rc=$prc $((($po | Out-String)).Trim())"

# evidence shot of the pasted input state
& $gx @("region:1590,830,880,140,2,$root\last_paste.png",'wait:200') | Out-Null
L "region rc=$LASTEXITCODE"

# 5) send
& $gx @('key:enter','wait:1500',"shot:$root\last_sent.jpg") | Out-Null
$src = $LASTEXITCODE
L "gx enter rc=$src"

# 6) end session (close badge + end notice + guard)
RunPS @('-File', (Join-Path $tools 'end-session.ps1')) | Out-Null
L "end-session rc=$script:LastRC"
if ($src -ne 0) { exit 12 }
L "OK task=$TaskId"
exit 0
