import os
import csv
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import re
from database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVIngestor:
    """
    CSV data ingestion and normalization for Email Guardian system
    Handles data cleaning, normalization, and bulk import to DuckDB
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.null_values = ['-', '', 'null', 'NULL', 'None', 'N/A', 'n/a']
        
    def normalize_value(self, value: Any) -> Optional[str]:
        """
        Normalize a single value - treat '-' and similar as null
        """
        if pd.isna(value) or str(value).strip() in self.null_values:
            return None
        return str(value).strip()
    
    def split_multi_values(self, value: str, delimiter: str = ',') -> List[str]:
        """
        Split comma-separated or semicolon-separated values
        """
        if not value or value in self.null_values:
            return []
        
        # Try different delimiters
        delimiters = [',', ';', '|']
        for delim in delimiters:
            if delim in value:
                return [item.strip() for item in value.split(delim) if item.strip()]
        
        return [value.strip()]
    
    def normalize_datetime(self, dt_value: Any) -> Optional[datetime]:
        """
        Convert various datetime formats to standard datetime object
        """
        if pd.isna(dt_value) or str(dt_value).strip() in self.null_values:
            return None
        
        dt_str = str(dt_value).strip()
        
        # Common datetime formats to try
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y %H:%M',
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        
        # Try pandas datetime parsing as fallback
        try:
            parsed = pd.to_datetime(dt_str)
            if not pd.isna(parsed):
                return parsed.to_pydatetime()
        except:
            pass
        
        logger.warning(f"Could not parse datetime: {dt_str}")
        return None
    
    def normalize_date(self, date_value: Any) -> Optional[str]:
        """
        Convert various date formats to standard YYYY-MM-DD format
        """
        if pd.isna(date_value) or str(date_value).strip() in self.null_values:
            return None
        
        date_str = str(date_value).strip()
        
        # Date formats to try
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%m-%d-%Y',
            '%d-%m-%Y'
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # Try pandas date parsing as fallback
        try:
            parsed = pd.to_datetime(date_str, errors='coerce')
            if not pd.isna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def normalize_email_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a single email record from CSV row
        """
        normalized = {}
        
        # Skip ID field - let database auto-generate
        
        # Normalize datetime fields
        if '_time' in row:
            normalized['_time'] = self.normalize_datetime(row['_time'])
        
        if 'termination_date' in row:
            normalized['termination_date'] = self.normalize_date(row['termination_date'])
        
        # Normalize text fields - only add if they exist in the CSV
        text_fields = ['sender', 'subject', 'time_month', 'leaver', 'bunit', 
                      'department', 'user_response', 'final_outcome']
        
        for field in text_fields:
            if field in row:
                normalized[field] = self.normalize_value(row[field])
        
        # Handle multi-value fields - only add if they exist in the CSV
        multi_value_fields = ['recipients', 'attachments', 'policy_name', 'justifications']
        
        for field in multi_value_fields:
            if field in row:
                raw_value = self.normalize_value(row[field])
                if raw_value:
                    # Split multi-values and store as comma-separated string
                    values = self.split_multi_values(raw_value)
                    normalized[field] = ', '.join(values) if values else None
        
        # Add creation timestamp
        normalized['created_at'] = datetime.now()
        
        # Log which fields were found for debugging
        logger.debug(f"Normalized record with fields: {list(normalized.keys())}")
        
        return normalized
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate that a record has minimum required fields
        """
        # At minimum, we need either sender or subject
        return bool(record.get('sender') or record.get('subject'))
    
    def process_csv_file(self, filepath: str) -> List[Dict[str, Any]]:
        """
        Process a single CSV file and return normalized records
        """
        logger.info(f"Processing CSV file: {filepath}")
        
        try:
            # Read CSV with pandas for better handling of various formats
            df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
            
            # Handle common encoding issues
            if df.empty:
                try:
                    df = pd.read_csv(filepath, encoding='latin-1', low_memory=False)
                except:
                    df = pd.read_csv(filepath, encoding='cp1252', low_memory=False)
            
            logger.info(f"Read {len(df)} rows from {filepath}")
            
            # Normalize column names (remove spaces, standardize case)
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            # Process each row
            normalized_records = []
            invalid_count = 0
            
            for idx, row in df.iterrows():
                try:
                    normalized_record = self.normalize_email_record(row.to_dict())
                    
                    if self.validate_record(normalized_record):
                        normalized_records.append(normalized_record)
                    else:
                        invalid_count += 1
                        logger.debug(f"Invalid record at row {idx}: {row.to_dict()}")
                        
                except Exception as e:
                    invalid_count += 1
                    logger.warning(f"Error processing row {idx}: {e}")
            
            logger.info(f"Normalized {len(normalized_records)} valid records, {invalid_count} invalid")
            return normalized_records
            
        except Exception as e:
            logger.error(f"Error processing CSV file {filepath}: {e}")
            return []
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert normalized records into the emails table
        """
        if not records:
            return 0
        
        try:
            conn = get_db_connection()
            
            # Get the actual columns from the first record to build dynamic insert
            if records:
                sample_record = records[0]
                available_fields = [field for field in [
                    '_time', 'sender', 'subject', 'attachments', 'recipients',
                    'time_month', 'leaver', 'termination_date', 'bunit', 'department',
                    'user_response', 'final_outcome', 'policy_name', 'justifications',
                    'created_at'
                ] if field in sample_record and sample_record[field] is not None]
                
                # Always include created_at if not present
                if 'created_at' not in available_fields:
                    available_fields.append('created_at')
            
            placeholders = ', '.join(['?' for _ in available_fields])
            insert_sql = f"""
                INSERT INTO emails ({', '.join(available_fields)})
                VALUES ({placeholders})
            """
            
            # Convert records to tuples for batch insert
            record_tuples = []
            for record in records:
                # Ensure created_at is set
                if 'created_at' not in record or record['created_at'] is None:
                    record['created_at'] = datetime.now()
                
                record_tuple = tuple(record.get(field) for field in available_fields)
                record_tuples.append(record_tuple)
            
            # Batch insert for better performance
            conn.executemany(insert_sql, record_tuples)
            
            # Get count of inserted records
            inserted_count = len(record_tuples)
            
            conn.close()
            logger.info(f"Successfully inserted {inserted_count} email records using fields: {available_fields}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            return 0
    
    def ingest_csv_files(self, file_pattern: str = "*.csv") -> Dict[str, int]:
        """
        Process all CSV files in the data directory
        """
        import glob
        
        results = {
            'files_processed': 0,
            'total_records': 0,
            'successful_inserts': 0,
            'errors': []
        }
        
        # Find all CSV files
        csv_files = glob.glob(os.path.join(self.data_dir, file_pattern))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.data_dir}")
            return results
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            try:
                logger.info(f"Processing file: {csv_file}")
                
                # Process CSV file
                normalized_records = self.process_csv_file(csv_file)
                results['total_records'] += len(normalized_records)
                
                # Insert records
                if normalized_records:
                    inserted_count = self.insert_records(normalized_records)
                    results['successful_inserts'] += inserted_count
                
                results['files_processed'] += 1
                
            except Exception as e:
                error_msg = f"Failed to process {csv_file}: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Log summary
        logger.info(f"""
        CSV Ingestion Summary:
        - Files processed: {results['files_processed']}
        - Total records found: {results['total_records']}
        - Successfully inserted: {results['successful_inserts']}
        - Errors: {len(results['errors'])}
        """)
        
        return results


def main():
    """
    Main function for command-line usage
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest CSV files into Email Guardian database')
    parser.add_argument('--data-dir', default='data', help='Directory containing CSV files')
    parser.add_argument('--pattern', default='*.csv', help='File pattern to match')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize ingestor
    ingestor = CSVIngestor(data_dir=args.data_dir)
    
    # Process files
    results = ingestor.ingest_csv_files(file_pattern=args.pattern)
    
    # Print summary
    print(f"\n📊 Ingestion Complete!")
    print(f"Files processed: {results['files_processed']}")
    print(f"Records found: {results['total_records']}")
    print(f"Successfully imported: {results['successful_inserts']}")
    
    if results['errors']:
        print(f"\n⚠️  Errors:")
        for error in results['errors']:
            print(f"  - {error}")


if __name__ == "__main__":
    main()