from typing import Dict

import boto3
from botocore.exceptions import ClientError


async def verify_s3_access(s3_config: Dict) -> bool:
    """
    Verify S3 access and permissions before starting scraper
    
    Args:
        s3_config: Dictionary containing:
            - bucket_name: S3 bucket name
            - base_path: Base path in bucket
            - region_name: AWS region
            - aws_access_key_id: AWS access key
            - aws_secret_access_key: AWS secret key
            
    Returns:
        bool: True if access is verified, False otherwise
    """
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=s3_config['aws_access_key_id'],
            aws_secret_access_key=s3_config['aws_secret_access_key'],
            region_name=s3_config['region_name']
        )
        
        # Test bucket access
        s3.head_bucket(Bucket=s3_config['bucket_name'])
        
        # Test write permission with a small file
        test_key = f"{s3_config['base_path'].rstrip('/')}/test_access.txt"
        s3.put_object(
            Bucket=s3_config['bucket_name'],
            Key=test_key,
            Body="Testing S3 access"
        )
        
        # Clean up test file
        s3.delete_object(
            Bucket=s3_config['bucket_name'],
            Key=test_key
        )
        
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            print(f"Error: Bucket {s3_config['bucket_name']} does not exist")
        elif error_code == '403':
            print("Error: Permission denied. Check your AWS credentials and bucket policies")
        else:
            print(f"AWS Error: {str(e)}")
        return False
    except Exception as e:
        print(f"Error verifying S3 access: {str(e)}")
        return False


def get_s3_client(s3_config: Dict) -> boto3.client:
    """
    Create an S3 client with the provided configuration
    
    Args:
        s3_config: Dictionary containing AWS credentials and configuration
        
    Returns:
        boto3.client: Configured S3 client
    """
    return boto3.client(
        's3',
        aws_access_key_id=s3_config['aws_access_key_id'],
        aws_secret_access_key=s3_config['aws_secret_access_key'],
        region_name=s3_config['region_name']
    )
