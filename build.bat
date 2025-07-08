@echo off
echo Building PostgreSQL to SQL Server Migration Tool...
echo.

REM Check if .NET is installed
dotnet --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: .NET 8.0 SDK is not installed or not in PATH
    echo Please install .NET 8.0 SDK from https://dotnet.microsoft.com/download
    pause
    exit /b 1
)

REM Restore packages
echo Restoring NuGet packages...
dotnet restore
if %errorlevel% neq 0 (
    echo Error: Failed to restore packages
    pause
    exit /b 1
)

REM Build the project
echo Building project...
dotnet build --configuration Release
if %errorlevel% neq 0 (
    echo Error: Build failed
    pause
    exit /b 1
)

echo.
echo Build completed successfully!
echo.
echo To run the migration tool:
echo 1. Copy appsettings.example.json to appsettings.json
echo 2. Update the connection strings in appsettings.json
echo 3. Run: dotnet run
echo.
pause 