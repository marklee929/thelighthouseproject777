@echo off
setlocal

:: Get the directory of this script
pushd %~dp0

echo [INFO] Handing over to PowerShell bootstrapper...

:: Start Ollama (Required for Crew Agents) to ensure it's running
echo [INFO] Awakening Llama (Ollama serve)...
start "Ollama Service" /min ollama serve

echo [INFO] Launching PowerShell script: run.ps1

powershell -NoProfile -ExecutionPolicy Bypass -File ".\run.ps1" %*

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] PowerShell script exited with error code %ERRORLEVEL%
    pause
)

popd
endlocal