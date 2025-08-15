
# Email Guardian - Local Installation Guide

## Overview
Email Guardian is a Flask-based email security and compliance management system that runs locally on your machine. This guide will walk you through the complete installation process.

## System Requirements

### Minimum Requirements
- **Operating System**: Windows 10+, macOS 10.14+, or Linux (Ubuntu 18.04+)
- **Python**: Version 3.11 or higher
- **Memory**: 2GB RAM minimum, 4GB recommended
- **Storage**: 1GB free space for application and data
- **Network**: Internet connection for initial package installation

### Recommended Setup
- **Python**: 3.11+ with pip package manager
- **Browser**: Chrome, Firefox, Safari, or Edge (latest versions)
- **Terminal**: Command prompt, PowerShell, or terminal application

## Installation Steps

### Step 1: Download the Application
1. Download or clone the Email Guardian application files
2. Extract to a directory of your choice (e.g., `C:\EmailGuardian` or `~/EmailGuardian`)

### Step 2: Install Python Dependencies
Navigate to the application directory and install required packages:

```bash
# Navigate to the application directory
cd /path/to/EmailGuardian

# Install dependencies using pip
pip install -r requirements.txt

# Alternative: Install from pyproject.toml
pip install -e .
```

### Step 3: Verify Directory Structure
Ensure your installation has the following folder structure:

```
EmailGuardian/
├── data/                   # CSV import files and processed data
├── logs/                   # Application logs and system messages
├── ml_models/             # Machine learning models and vectorizers
├── reports/               # Generated PDF and Excel reports
│   └── charts/           # Chart images for reports
├── static/               # Web assets (CSS, JavaScript)
│   ├── css/
│   └── js/
├── templates/            # HTML templates
├── app.py               # Flask application factory
├── main.py              # Application entry point
├── database.py          # Database management
├── routes.py            # Web routes and API endpoints
├── ml_models.py         # Machine learning components
├── reports.py           # Report generation
└── requirements.txt     # Python dependencies
```

### Step 4: Initialize the Application
Run the application for the first time to create the database:

```bash
python main.py
```

You should see output similar to:
```
INFO:root:Database initialized successfully
* Running on all addresses (0.0.0.0)
* Running on http://127.0.0.1:5000
* Running on http://[your-ip]:5000
```

## Required Folders and Permissions

### Critical Directories
The application requires these directories to exist with write permissions:

1. **`data/`** - Stores uploaded CSV files and processed email data
   - Must be writable for CSV imports
   - Contains processed email datasets

2. **`logs/`** - Application logging and system messages
   - Must be writable for log files
   - Stores error logs and activity tracking

3. **`ml_models/`** - Machine learning models and training data
   - Must be writable for model persistence
   - Stores trained classifiers and vectorizers

4. **`reports/`** - Generated reports and chart images
   - Must be writable for PDF and Excel generation
   - Subdirectory `charts/` for temporary chart images

### Auto-Creation
The application will automatically create these directories if they don't exist, but ensure the parent directory has write permissions.

## Database Setup

### Automatic Initialization
- The application uses DuckDB (local file database)
- Database file: `email_guardian.db` (created automatically)
- No manual database setup required
- All tables are created on first run

### Database Location
The database file will be created in the application root directory. Ensure this location has write permissions.

## First Run and Access

### Starting the Application
```bash
# From the application directory
python main.py
```

### Accessing the Web Interface
1. Open your web browser
2. Navigate to: `http://localhost:5000`
3. You should see the Email Guardian dashboard

### Default Configuration
- **Port**: 5000 (configurable in main.py)
- **Host**: All interfaces (0.0.0.0)
- **Debug Mode**: Enabled for development

## Data Import Setup

### CSV File Requirements
1. Place CSV files in the `data/` directory
2. Required columns in CSV:
   - `Sender`
   - `Subject`
   - `Body` or `Content`
   - `Date` (optional)
   - `Category` (optional)

### Sample CSV Format
```csv
Sender,Subject,Body,Date,Category
john@company.com,Budget Review,Please review the attached budget...,2024-01-15,Finance
legal@firm.com,Contract Amendment,The contract needs the following changes...,2024-01-16,Legal
```

## Troubleshooting

### Common Issues

#### Port Already in Use
```bash
# Error: Address already in use
# Solution: Use a different port
python main.py --port 5001
```

#### Permission Denied Errors
```bash
# Ensure write permissions for:
chmod 755 data/ logs/ ml_models/ reports/
```

#### Missing Dependencies
```bash
# Reinstall requirements
pip install -r requirements.txt --force-reinstall
```

#### Database Issues
```bash
# If database corruption occurs, delete and restart:
rm email_guardian.db
python main.py
```

### Log Files
Check application logs for detailed error information:
- `logs/application.log` - General application logs
- `logs/error.log` - Error messages and stack traces

## Production Deployment

### For Production Use
1. Set `debug=False` in main.py
2. Use a production WSGI server like Gunicorn:
   ```bash
   pip install gunicorn
   gunicorn --bind 0.0.0.0:5000 main:app
   ```
3. Configure proper backup procedures for the database
4. Set up log rotation for the logs directory

### Security Considerations
- Change the default secret key in production
- Implement proper authentication if needed
- Secure file upload directory permissions
- Regular database backups

## Support and Maintenance

### Regular Maintenance
1. **Database Backups**: Copy `email_guardian.db` regularly
2. **Log Cleanup**: Archive old log files from `logs/` directory
3. **Model Retraining**: The ML models update automatically with new data
4. **Report Cleanup**: Remove old reports from `reports/` directory

### Getting Help
- Check application logs in the `logs/` directory
- Verify all required directories exist and are writable
- Ensure Python dependencies are correctly installed
- Test database connectivity through the admin panel

## Quick Start Checklist

- [ ] Python 3.11+ installed
- [ ] Application files extracted to desired location
- [ ] Dependencies installed via pip
- [ ] All required directories exist with write permissions
- [ ] Database initializes successfully on first run
- [ ] Web interface accessible at http://localhost:5000
- [ ] CSV import functionality tested
- [ ] Admin panel accessible for configuration

Your Email Guardian installation is now complete and ready for use!
