#!/usr/bin/env python3
"""
AI Academy Tracker - S3 Data Monitoring Script
Monitors S3 bucket usage, data integrity, and system health
"""
import boto3
import pandas as pd
from datetime import datetime, timedelta
import json
import sys
from io import StringIO

# Configuration
S3_BUCKET_NAME = 'ai-academy-tracker-uploads'
AWS_REGION = 'us-east-1'
S3_CURRENT_DATA_KEY = 'processed-data/current/employee.csv'
S3_BACKUP_PREFIX = 'processed-data/backups/'
S3_RAW_UPLOADS_PREFIX = 'raw-uploads/'

def get_s3_client():
    """Initialize S3 client"""
    try:
        return boto3.client('s3', region_name=AWS_REGION)
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {e}")
        sys.exit(1)

def check_bucket_exists(s3_client):
    """Check if the S3 bucket exists and is accessible"""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f"✅ S3 bucket '{S3_BUCKET_NAME}' is accessible")
        return True
    except Exception as e:
        print(f"❌ S3 bucket '{S3_BUCKET_NAME}' is not accessible: {e}")
        return False

def get_current_data_info(s3_client):
    """Get information about current employee data"""
    try:
        # Check if current data exists
        response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=S3_CURRENT_DATA_KEY)
        
        last_modified = response['LastModified']
        size_bytes = response['ContentLength']
        size_mb = round(size_bytes / (1024 * 1024), 2)
        
        # Read and analyze the data
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_CURRENT_DATA_KEY)
        csv_content = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        total_records = len(df)
        unique_employees = len(df['Employee_ID'].unique()) if 'Employee_ID' in df.columns else 0
        
        print(f"✅ Current Data Status:")
        print(f"   📁 File: {S3_CURRENT_DATA_KEY}")
        print(f"   📊 Records: {total_records:,}")
        print(f"   👥 Unique Employees: {unique_employees:,}")
        print(f"   📏 Size: {size_mb} MB")
        print(f"   🕐 Last Modified: {last_modified}")
        
        return {
            'exists': True,
            'records': total_records,
            'employees': unique_employees,
            'size_mb': size_mb,
            'last_modified': last_modified
        }
        
    except s3_client.exceptions.NoSuchKey:
        print(f"⚠️ No current employee data found at {S3_CURRENT_DATA_KEY}")
        return {'exists': False}
    except Exception as e:
        print(f"❌ Error checking current data: {e}")
        return {'exists': False}

def list_backups(s3_client):
    """List available backups"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_BACKUP_PREFIX
        )
        
        if 'Contents' not in response:
            print(f"📂 No backups found in {S3_BACKUP_PREFIX}")
            return []
        
        backups = []
        total_backup_size = 0
        
        for obj in response['Contents']:
            key = obj['Key']
            size_mb = round(obj['Size'] / (1024 * 1024), 2)
            modified = obj['LastModified']
            
            backups.append({
                'key': key,
                'size_mb': size_mb,
                'modified': modified
            })
            total_backup_size += size_mb
        
        print(f"📂 Backup Status:")
        print(f"   📊 Total Backups: {len(backups)}")
        print(f"   📏 Total Size: {total_backup_size:.2f} MB")
        
        # Show most recent backups
        recent_backups = sorted(backups, key=lambda x: x['modified'], reverse=True)[:5]
        print(f"   🔄 Recent Backups:")
        for backup in recent_backups:
            filename = backup['key'].split('/')[-1]
            print(f"     • {filename} ({backup['size_mb']} MB) - {backup['modified']}")
        
        return backups
        
    except Exception as e:
        print(f"❌ Error listing backups: {e}")
        return []

def check_raw_uploads(s3_client):
    """Check raw uploads that haven't been processed"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_RAW_UPLOADS_PREFIX
        )
        
        if 'Contents' not in response:
            print(f"📤 No raw uploads found")
            return []
        
        uploads = []
        total_size = 0
        old_uploads = 0
        
        cutoff_date = datetime.now(response['Contents'][0]['LastModified'].tzinfo) - timedelta(hours=2)
        
        for obj in response['Contents']:
            key = obj['Key']
            size_mb = round(obj['Size'] / (1024 * 1024), 2)
            modified = obj['LastModified']
            
            uploads.append({
                'key': key,
                'size_mb': size_mb,
                'modified': modified
            })
            total_size += size_mb
            
            if modified < cutoff_date:
                old_uploads += 1
        
        print(f"📤 Raw Uploads Status:")
        print(f"   📊 Total Files: {len(uploads)}")
        print(f"   📏 Total Size: {total_size:.2f} MB")
        
        if old_uploads > 0:
            print(f"   ⚠️ Old files (>2 hours): {old_uploads} - Consider cleanup")
        
        return uploads
        
    except Exception as e:
        print(f"❌ Error checking raw uploads: {e}")
        return []

def get_bucket_metrics(s3_client):
    """Get overall bucket metrics"""
    try:
        # Get bucket size using CloudWatch (requires CloudWatch metrics enabled)
        cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=1)
        
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketSizeBytes',
                Dimensions=[
                    {'Name': 'BucketName', 'Value': S3_BUCKET_NAME},
                    {'Name': 'StorageType', 'Value': 'StandardStorage'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                size_bytes = response['Datapoints'][-1]['Average']
                size_gb = round(size_bytes / (1024 * 1024 * 1024), 2)
                print(f"📊 Bucket Metrics:")
                print(f"   📏 Total Size: {size_gb} GB")
            else:
                print(f"📊 Bucket metrics not available (enable S3 metrics in console)")
                
        except Exception as cw_error:
            print(f"📊 CloudWatch metrics unavailable: {cw_error}")
        
    except Exception as e:
        print(f"❌ Error getting bucket metrics: {e}")

def check_data_integrity(s3_client):
    """Basic data integrity checks"""
    try:
        # Get current data
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_CURRENT_DATA_KEY)
        csv_content = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"🔍 Data Integrity Check:")
        
        # Check required columns
        required_columns = ['Employee_ID', 'Level', 'Qualifier', 'Valid_Till', 'Wipro_Function']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"   ❌ Missing required columns: {missing_columns}")
        else:
            print(f"   ✅ All required columns present")
        
        # Check for empty values
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            print(f"   ⚠️ Null values found:")
            for col, count in null_counts[null_counts > 0].items():
                print(f"     • {col}: {count} null values")
        else:
            print(f"   ✅ No null values found")
        
        # Check level distribution
        if 'Level' in df.columns:
            level_counts = df['Level'].value_counts()
            print(f"   📊 Level Distribution:")
            for level, count in level_counts.items():
                print(f"     • {level}: {count:,} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Data integrity check failed: {e}")
        return False

def cleanup_old_raw_uploads(s3_client, dry_run=True):
    """Clean up raw uploads older than 24 hours"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_RAW_UPLOADS_PREFIX
        )
        
        if 'Contents' not in response:
            print(f"🧹 No raw uploads to clean up")
            return
        
        cutoff_date = datetime.now(response['Contents'][0]['LastModified'].tzinfo) - timedelta(hours=24)
        old_files = []
        
        for obj in response['Contents']:
            if obj['LastModified'] < cutoff_date:
                old_files.append(obj['Key'])
        
        if not old_files:
            print(f"🧹 No old raw uploads found (older than 24 hours)")
            return
        
        print(f"🧹 Cleanup Summary:")
        print(f"   📊 Files to delete: {len(old_files)}")
        
        if dry_run:
            print(f"   🚫 DRY RUN - No files will be deleted")
            print(f"   Files that would be deleted:")
            for key in old_files[:5]:  # Show first 5
                print(f"     • {key}")
            if len(old_files) > 5:
                print(f"     ... and {len(old_files) - 5} more")
        else:
            # Actually delete files
            deleted_count = 0
            for key in old_files:
                try:
                    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
                    deleted_count += 1
                except Exception as e:
                    print(f"   ❌ Failed to delete {key}: {e}")
            
            print(f"   ✅ Successfully deleted {deleted_count} old files")
    
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

def main():
    """Main monitoring function"""
    print("=" * 80)
    print("🔍 AI Academy Tracker - S3 Data Monitoring")
    print("=" * 80)
    print(f"📅 Timestamp: {datetime.now()}")
    print(f"🪣 Bucket: {S3_BUCKET_NAME}")
    print(f"🌍 Region: {AWS_REGION}")
    print("-" * 80)
    
    # Initialize S3 client
    s3_client = get_s3_client()
    
    # Check bucket accessibility
    if not check_bucket_exists(s3_client):
        sys.exit(1)
    
    print()
    
    # Check current data
    current_data_info = get_current_data_info(s3_client)
    print()
    
    # List backups
    backups = list_backups(s3_client)
    print()
    
    # Check raw uploads
    raw_uploads = check_raw_uploads(s3_client)
    print()
    
    # Get bucket metrics
    get_bucket_metrics(s3_client)
    print()
    
    # Data integrity check
    if current_data_info.get('exists'):
        check_data_integrity(s3_client)
        print()
    
    # Cleanup old uploads (dry run by default)
    cleanup_old_raw_uploads(s3_client, dry_run=True)
    print()
    
    # Summary
    print("=" * 80)
    print("📋 SUMMARY")
    print("=" * 80)
    
    if current_data_info.get('exists'):
        print(f"✅ Current Data: {current_data_info['records']:,} records, {current_data_info['employees']:,} employees")
    else:
        print(f"❌ Current Data: Not available")
    
    print(f"📂 Backups: {len(backups)} available")
    print(f"📤 Raw Uploads: {len(raw_uploads)} pending")
    
    print()
    print("🚀 Monitoring complete!")

if __name__ == "__main__":
    main()
