# Email Guardian

## Overview

Email Guardian is a Flask-based email monitoring and analysis system designed for enterprise compliance and security. The application processes high-volume email data (10,000+ records per day), identifies potential policy violations through machine learning classification, and provides comprehensive case management for security escalations. It features real-time analytics dashboards, automated reporting capabilities, and administrative rule configuration for proactive email monitoring.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture
- **Framework**: Flask web application with modular route organization
- **Database**: DuckDB for high-performance analytics and email record storage
- **Data Models**: Dataclass-based models for Email, Case, FlaggedSender, and AdminRule entities
- **Session Management**: Flask sessions with configurable secret key for authentication

### Database Design
- **Primary Tables**: 
  - `emails` table stores comprehensive email metadata including sender, subject, attachments, recipients, and business unit information
  - `cases` table tracks escalated emails with status management and foreign key relationships
- **Schema Features**: Timestamp tracking, department categorization, policy violation tracking, and audit trail capabilities

### Machine Learning Pipeline
- **Classification Engine**: Scikit-learn based email classifier using TF-IDF vectorization and Naive Bayes
- **Model Persistence**: Pickle-based model and vectorizer storage in `ml_models/` directory
- **Training Pipeline**: Automated retraining on historical email outcomes and policy decisions
- **Feature Engineering**: Text analysis of email subjects and justification content

### Frontend Architecture
- **UI Framework**: Bootstrap 5 with dark theme optimization
- **JavaScript Libraries**: Chart.js for analytics visualization, DataTables for data management
- **Template Engine**: Jinja2 with modular template inheritance
- **Responsive Design**: Mobile-first approach with adaptive layouts

### Data Processing Pipeline
- **CSV Import**: Bulk data ingestion with validation and error handling
- **Real-time Processing**: Live email classification and automatic case creation
- **Batch Operations**: Scheduled model retraining and bulk data exports

### Reporting System
- **PDF Generation**: ReportLab-based professional reports with charts and tables
- **Excel Export**: Pandas-powered data export with formatting
- **Automated Scheduling**: Configurable report generation with date range filtering

### Security Features
- **Sender Flagging**: Dynamic blacklist management with reason tracking
- **Admin Rules Engine**: Configurable business rules for automated email filtering
- **Case Management**: Workflow-based escalation with status tracking

## External Dependencies

### Core Framework Dependencies
- **Flask**: Web application framework for routing and templating
- **DuckDB**: High-performance analytical database for email data storage
- **Pandas**: Data manipulation and analysis for CSV processing and reporting
- **Scikit-learn**: Machine learning library for email classification and NLP features

### Data Visualization
- **Chart.js**: Interactive charts and dashboards for email analytics
- **DataTables**: Advanced table features with search, pagination, and sorting
- **Bootstrap 5**: UI framework with dark theme and responsive design

### Report Generation
- **ReportLab**: PDF report generation with charts, tables, and professional formatting
- **Excel Libraries**: Data export capabilities for compliance reporting

### Frontend Assets
- **Font Awesome**: Icon library for consistent UI elements
- **Bootstrap CDN**: CSS framework delivered via CDN for reliability
- **DataTables CDN**: Table enhancement library for data management interfaces

### Development Tools
- **Werkzeug**: WSGI utilities for file upload handling and security
- **Logging**: Python standard library for application monitoring and debugging