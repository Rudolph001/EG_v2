
#!/usr/bin/env python3
"""
Email Guardian Application Packager
Creates standalone executables for Windows and Mac with all dependencies included
"""

import os
import sys
import shutil
import subprocess
import platform
from pathlib import Path

def ensure_pyinstaller():
    """Ensure PyInstaller is installed"""
    try:
        import PyInstaller
        print("✓ PyInstaller already installed")
    except ImportError:
        print("Installing PyInstaller...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        print("✓ PyInstaller installed successfully")

def create_spec_file():
    """Create PyInstaller spec file"""
    spec_content = '''
# -*- mode: python ; coding: utf-8 -*-
import sys
from pathlib import Path

block_cipher = None

# Get all data files
data_files = []

# Add templates
template_dir = Path('templates')
if template_dir.exists():
    for template in template_dir.rglob('*.html'):
        data_files.append((str(template), 'templates'))

# Add static files
static_dir = Path('static')
if static_dir.exists():
    for static_file in static_dir.rglob('*'):
        if static_file.is_file():
            rel_path = static_file.relative_to(static_dir)
            data_files.append((str(static_file), f'static/{rel_path.parent}'))

# Add data directory structure
for dir_name in ['data', 'logs', 'ml_models', 'reports']:
    dir_path = Path(dir_name)
    if dir_path.exists():
        data_files.append((str(dir_path / '.gitkeep'), dir_name))

# Hidden imports for all modules
hidden_imports = [
    'flask',
    'duckdb',
    'pandas',
    'numpy',
    'scikit-learn',
    'matplotlib',
    'seaborn',
    'reportlab',
    'openpyxl',
    'joblib',
    'email_validator',
    'werkzeug',
    'jinja2',
    'click',
    'itsdangerous',
    'markupsafe',
    'blinker'
]

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=data_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='EmailGuardian',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
'''
    
    with open('email_guardian.spec', 'w') as f:
        f.write(spec_content.strip())
    
    print("✓ PyInstaller spec file created")

def build_executable():
    """Build the executable using PyInstaller"""
    print(f"Building executable for {platform.system()}...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "PyInstaller", 
            "--clean", 
            "email_guardian.spec"
        ])
        print("✓ Executable built successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Build failed: {e}")
        return False

def create_installer_script():
    """Create installer scripts for different platforms"""
    
    # Windows batch installer
    windows_installer = '''@echo off
echo Installing Email Guardian...
echo.

REM Create application directory
if not exist "%USERPROFILE%\\EmailGuardian" mkdir "%USERPROFILE%\\EmailGuardian"

REM Copy executable
copy "EmailGuardian.exe" "%USERPROFILE%\\EmailGuardian\\" > nul
if errorlevel 1 (
    echo Error: Failed to copy executable
    pause
    exit /b 1
)

REM Create data directories
mkdir "%USERPROFILE%\\EmailGuardian\\data" 2>nul
mkdir "%USERPROFILE%\\EmailGuardian\\logs" 2>nul
mkdir "%USERPROFILE%\\EmailGuardian\\ml_models" 2>nul
mkdir "%USERPROFILE%\\EmailGuardian\\reports" 2>nul

REM Create desktop shortcut
echo Creating desktop shortcut...
set "shortcut=%USERPROFILE%\\Desktop\\Email Guardian.lnk"
powershell "$WshShell = New-Object -comObject WScript.Shell; $Shortcut = $WshShell.CreateShortcut('%shortcut%'); $Shortcut.TargetPath = '%USERPROFILE%\\EmailGuardian\\EmailGuardian.exe'; $Shortcut.WorkingDirectory = '%USERPROFILE%\\EmailGuardian'; $Shortcut.Save()"

REM Create start menu entry
set "startmenu=%APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Email Guardian.lnk"
powershell "$WshShell = New-Object -comObject WScript.Shell; $Shortcut = $WshShell.CreateShortcut('%startmenu%'); $Shortcut.TargetPath = '%USERPROFILE%\\EmailGuardian\\EmailGuardian.exe'; $Shortcut.WorkingDirectory = '%USERPROFILE%\\EmailGuardian'; $Shortcut.Save()"

echo.
echo ✓ Email Guardian installed successfully!
echo ✓ Desktop shortcut created
echo ✓ Start menu entry created
echo.
echo The application will be available at: %USERPROFILE%\\EmailGuardian\\
echo You can access it via the desktop shortcut or start menu.
echo.
pause
'''

    # Mac installer script
    mac_installer = '''#!/bin/bash
echo "Installing Email Guardian..."
echo

# Create application directory
APP_DIR="$HOME/Applications/EmailGuardian"
mkdir -p "$APP_DIR"

# Copy executable
cp "EmailGuardian" "$APP_DIR/" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Failed to copy executable"
    read -p "Press any key to continue..."
    exit 1
fi

# Make executable
chmod +x "$APP_DIR/EmailGuardian"

# Create data directories
mkdir -p "$APP_DIR/data"
mkdir -p "$APP_DIR/logs"
mkdir -p "$APP_DIR/ml_models" 
mkdir -p "$APP_DIR/reports"

# Create application bundle structure
BUNDLE_DIR="$HOME/Applications/Email Guardian.app"
mkdir -p "$BUNDLE_DIR/Contents/MacOS"
mkdir -p "$BUNDLE_DIR/Contents/Resources"

# Copy executable to bundle
cp "$APP_DIR/EmailGuardian" "$BUNDLE_DIR/Contents/MacOS/"

# Create Info.plist
cat > "$BUNDLE_DIR/Contents/Info.plist" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>EmailGuardian</string>
    <key>CFBundleIdentifier</key>
    <string>com.emailguardian.app</string>
    <key>CFBundleName</key>
    <string>Email Guardian</string>
    <key>CFBundleVersion</key>
    <string>1.0.0</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
</dict>
</plist>
EOF

# Create launcher script that sets working directory
cat > "$BUNDLE_DIR/Contents/MacOS/EmailGuardianLauncher" << EOF
#!/bin/bash
cd "$APP_DIR"
exec "./EmailGuardian"
EOF

chmod +x "$BUNDLE_DIR/Contents/MacOS/EmailGuardianLauncher"

# Update Info.plist to use launcher
sed -i '' 's/<string>EmailGuardian<\\/string>/<string>EmailGuardianLauncher<\\/string>/' "$BUNDLE_DIR/Contents/Info.plist"

echo
echo "✓ Email Guardian installed successfully!"
echo "✓ Application bundle created at: $BUNDLE_DIR"
echo
echo "The application is now available in your Applications folder."
echo "You can also run it directly from: $APP_DIR/EmailGuardian"
echo
read -p "Press any key to continue..."
'''

    # Linux installer script
    linux_installer = '''#!/bin/bash
echo "Installing Email Guardian..."
echo

# Create application directory
APP_DIR="$HOME/.local/share/EmailGuardian"
mkdir -p "$APP_DIR"

# Copy executable
cp "EmailGuardian" "$APP_DIR/" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Failed to copy executable"
    read -p "Press any key to continue..."
    exit 1
fi

# Make executable
chmod +x "$APP_DIR/EmailGuardian"

# Create data directories
mkdir -p "$APP_DIR/data"
mkdir -p "$APP_DIR/logs" 
mkdir -p "$APP_DIR/ml_models"
mkdir -p "$APP_DIR/reports"

# Create desktop entry
DESKTOP_FILE="$HOME/.local/share/applications/email-guardian.desktop"
mkdir -p "$(dirname "$DESKTOP_FILE")"

cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Name=Email Guardian
Comment=Email Security and Compliance Management System
Exec=$APP_DIR/EmailGuardian
Icon=$APP_DIR/icon.png
Terminal=true
Type=Application
Categories=Office;Security;
EOF

# Create bin symlink for command line access
mkdir -p "$HOME/.local/bin"
ln -sf "$APP_DIR/EmailGuardian" "$HOME/.local/bin/email-guardian"

echo
echo "✓ Email Guardian installed successfully!"
echo "✓ Desktop entry created"
echo "✓ Command line access: email-guardian"
echo
echo "The application is installed at: $APP_DIR"
echo
read -p "Press any key to continue..."
'''

    # Write installer scripts
    with open('install_windows.bat', 'w') as f:
        f.write(windows_installer.strip())
    
    with open('install_mac.sh', 'w') as f:
        f.write(mac_installer.strip())
    
    with open('install_linux.sh', 'w') as f:
        f.write(linux_installer.strip())
    
    # Make shell scripts executable on Unix systems
    if platform.system() != 'Windows':
        os.chmod('install_mac.sh', 0o755)
        os.chmod('install_linux.sh', 0o755)
    
    print("✓ Installer scripts created")

def create_readme():
    """Create README for the package"""
    readme_content = '''# Email Guardian - Standalone Application

## Overview
Email Guardian is a comprehensive email security and compliance management system that helps organizations monitor, analyze, and manage email communications for policy compliance and security threats.

## Features
- **Email Monitoring**: Real-time email analysis and classification
- **ML-Powered Risk Assessment**: Advanced machine learning models for threat detection
- **Compliance Management**: Policy enforcement and audit trail
- **Reporting System**: Professional PDF and Excel reports
- **Admin Panel**: Comprehensive rule and configuration management
- **Follow-up Integration**: Automated email response system

## Installation

### Windows
1. Download the Windows package
2. Extract all files to a folder
3. Right-click `install_windows.bat` and select "Run as administrator"
4. Follow the installation prompts
5. Launch from Desktop shortcut or Start Menu

### Mac
1. Download the Mac package
2. Extract all files to a folder
3. Open Terminal and navigate to the extracted folder
4. Run: `chmod +x install_mac.sh && ./install_mac.sh`
5. Launch from Applications folder

### Linux
1. Download the Linux package
2. Extract all files to a folder
3. Open Terminal and navigate to the extracted folder
4. Run: `chmod +x install_linux.sh && ./install_linux.sh`
5. Launch from Applications menu or run `email-guardian` from terminal

## Usage
1. Start the application
2. Open your web browser and go to `http://localhost:5000`
3. Upload CSV files with email data through the Import Data section
4. Configure rules and policies in the Admin Panel
5. Monitor dashboards for security insights and compliance status

## System Requirements
- **Operating System**: Windows 10+, macOS 10.14+, or Linux (Ubuntu 18.04+)
- **Memory**: 2GB RAM minimum, 4GB recommended
- **Storage**: 1GB free space
- **Network**: Internet connection for initial setup and updates

## Data Security
- All data is stored locally on your machine
- No external data transmission without explicit user action
- Database files are stored in the application directory
- Logs and reports are saved locally

## Support
For technical support or questions:
- Check the application logs in the `logs` directory
- Review the database status in the Admin Panel
- Ensure all required directories exist and are writable

## Troubleshooting
- **Port conflicts**: If port 5000 is in use, the application will show an error
- **Permission issues**: Ensure the application has write access to its directory
- **Database issues**: Delete `email_guardian.db` to reset (will lose data)
- **Missing dependencies**: Reinstall using the appropriate installer script

Version: 1.0.0
Build Date: ''' + str(subprocess.check_output(['date'], shell=True, text=True).strip() if platform.system() != 'Windows' else 'Built with PyInstaller')

    with open('README.txt', 'w') as f:
        f.write(readme_content)
    
    print("✓ README created")

def create_startup_script():
    """Create a startup script that ensures proper working directory"""
    startup_content = '''
import os
import sys
from pathlib import Path

# Ensure we're in the correct directory
if getattr(sys, 'frozen', False):
    # Running as PyInstaller bundle
    application_path = Path(sys.executable).parent
    os.chdir(application_path)
    
    # Create required directories if they don't exist
    for dirname in ['data', 'logs', 'ml_models', 'reports']:
        dirpath = application_path / dirname
        dirpath.mkdir(exist_ok=True)
        
        # Create .gitkeep files
        gitkeep = dirpath / '.gitkeep'
        if not gitkeep.exists():
            gitkeep.touch()

# Import the main application
from main import app

if __name__ == "__main__":
    print("🚀 Starting Email Guardian...")
    print(f"📁 Working directory: {os.getcwd()}")
    print("🌐 Access the application at: http://localhost:5000")
    print("⏹️  Press Ctrl+C to stop the server")
    print()
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\\n👋 Email Guardian stopped")
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        input("Press Enter to exit...")
'''
def create_startup_script():
    dist_dir = os.path.join(os.getcwd(), "dist")
    os.makedirs(dist_dir, exist_ok=True)
    startup_script_path = os.path.join(dist_dir, "start_email_guardian.bat")

    startup_content = """@echo off
python app.py
pause
"""
    with open(startup_script_path, "w", encoding="utf-8") as f:
        f.write(startup_content.strip())
    #with open('startup.py', 'w') as f:
    #    f.write(startup_content.strip())
    
    print("✓ Startup script created")

def package_application():
    """Main packaging function"""
    print("📦 Email Guardian Packaging Tool")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists('main.py'):
        print("❌ Error: main.py not found. Please run this script from the application root directory.")
        return False
    
    # Install PyInstaller
    ensure_pyinstaller()
    
    # Create startup script
    create_startup_script()
    
    # Create spec file
    create_spec_file()
    
    # Build executable
    if not build_executable():
        return False
    
    # Create installer scripts
    create_installer_script()
    
    # Create README
    create_readme()
    
    # Create distribution package
    dist_dir = Path('dist')
    package_dir = dist_dir / 'EmailGuardian_Package'
    package_dir.mkdir(exist_ok=True)
    
    # Copy executable
    executable_name = 'EmailGuardian.exe' if platform.system() == 'Windows' else 'EmailGuardian'
    exe_path = dist_dir / executable_name
    
    if exe_path.exists():
        shutil.copy(exe_path, package_dir / executable_name)
    
    # Copy installer scripts
    installer_files = ['install_windows.bat', 'install_mac.sh', 'install_linux.sh', 'README.txt']
    for filename in installer_files:
        if os.path.exists(filename):
            shutil.copy(filename, package_dir / filename)
    
    # Create sample data file
    sample_data = package_dir / 'sample_data.csv'
    with open(sample_data, 'w') as f:
        f.write('_time,sender,subject,recipients,department,attachments\n')
        f.write('2024-01-15 10:30:00,user@company.com,Monthly Report,team@company.com,Finance,report.pdf\n')
        f.write('2024-01-15 11:45:00,external@partner.com,Contract Review,legal@company.com,Legal,contract.docx\n')
    
    print(f"✓ Package created successfully at: {package_dir}")
    print(f"📁 Package size: {sum(f.stat().st_size for f in package_dir.rglob('*') if f.is_file()) / (1024*1024):.1f} MB")
    
    print("\n🎉 Packaging Complete!")
    print("=" * 50)
    print("Next steps:")
    print(f"1. Navigate to: {package_dir}")
    print("2. Distribute the entire folder to end users")
    print("3. Users should run the appropriate installer script")
    print("4. The application will be accessible at http://localhost:5000")
    
    return True

if __name__ == "__main__":
    success = package_application()
    if not success:
        input("❌ Packaging failed. Press Enter to exit...")
        sys.exit(1)
    else:
        input("✅ Packaging completed successfully! Press Enter to exit...")
