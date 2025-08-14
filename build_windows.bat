
@echo off
echo Building Email Guardian for Windows...
echo.

REM Install packaging requirements
echo Installing packaging dependencies...
pip install -r requirements_packaging.txt
if errorlevel 1 (
    echo Error installing dependencies
    pause
    exit /b 1
)

REM Run the packaging script
echo Running packaging script...
python build_package.py

echo.
echo Build complete! Check the dist/EmailGuardian_Package folder.
pause
