@echo off
REM Start the LLM node on Windows.
REM Usage: start.bat  |  start.bat --peers 192.168.1.20:8080
cd /d "%~dp0"
python node.py %*
