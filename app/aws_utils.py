# app/aws_utils.py

import logging
import os
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
    from botocore.config import Config as BotoConfig
    BEDROCK_AVAILABLE = True
    AWS_AVAILABLE = True
    logging.info("✅ boto3 and botocore successfully imported")
except ImportError as e:
    BEDROCK_AVAILABLE = False
    AWS_AVAILABLE = False
    logging.warning(f"⚠️ boto3/botocore not available: {e}")
    BotoConfig = None
    ClientError = Exception  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS configuration
# ---------------------------------------------------------------------------

BEDROCK_ENABLED = os.getenv('BEDROCK_ENABLED', 'true').lower() == 'true'
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# IMPORTANT:
# Use a MODEL ID here, not an ARN.
# Example: "amazon.nova-pro-v1:0"
BEDROCK_MODEL_ID = os.getenv(
    'BEDROCK_MODEL_ID',
    'amazon.nova-pro-v1:0'
)

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'ai-academy-tracker-uploads')
S3_ENABLED = os.getenv('S3_ENABLED', 'true').lower() == 'true'

# S3 keys for data storage
S3_CURRENT_DATA_KEY = 'processed-data/current/employee.csv'
S3_BACKUP_PREFIX = 'processed-data/backups/'
S3_RAW_UPLOADS_PREFIX = 'raw-uploads/'

# AWS client config
aws_config = None
if AWS_AVAILABLE and BotoConfig is not None:
    aws_config = BotoConfig(
        region_name=AWS_REGION,
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        max_pool_connections=50,
        read_timeout=3600,
    )


# ---------------------------------------------------------------------------
# Bedrock / MagicalDataAgent
# ---------------------------------------------------------------------------

class MagicalDataAgent:
    """AI Agent wrapper with Bedrock integration."""

    def __init__(self):
        self.bedrock_client = None
        self.bedrock_status = "Not Initialized"

        if BEDROCK_AVAILABLE and BEDROCK_ENABLED and aws_config is not None:
            try:
                logger.info("🔧 Initializing AWS Bedrock client...")
                self.bedrock_client = boto3.client(
                    service_name='bedrock-runtime',
                    region_name=AWS_REGION,
                    config=aws_config
                )
                self._test_bedrock_connection()
            except Exception as e:
                logger.error(f"❌ Bedrock initialization error: {e}")
                self.bedrock_status = f"Error: {str(e)}"
        else:
            if not BEDROCK_AVAILABLE:
                self.bedrock_status = "boto3 not installed"
            elif not BEDROCK_ENABLED:
                self.bedrock_status = "Disabled in config"

    def _test_bedrock_connection(self):
        """Test Bedrock connection (lightweight)."""
        try:
            self.bedrock_status = "Ready - Model Available"
            logger.info("✅ Bedrock client successfully initialized!")
        except Exception as e:
            logger.warning(f"⚠️ Could not test Bedrock: {e}")
            self.bedrock_status = "Connected - Cannot Test"


# ---------------------------------------------------------------------------
# S3 File Manager
# ---------------------------------------------------------------------------

class S3FileManager:
    """S3 operations for uploads and storage only."""

    def __init__(self):
        self.s3_client = None
        self.s3_status = "Not Initialized"

        if AWS_AVAILABLE and S3_ENABLED and aws_config is not None:
            try:
                logger.info("🔧 Initializing S3 client...")
                self.s3_client = boto3.client(
                    's3',
                    region_name=AWS_REGION,
                    config=aws_config
                )
                self._test_s3_access()
            except Exception as e:
                logger.error(f"❌ S3 initialization error: {e}")
                self.s3_status = f"Error: {str(e)}"
        else:
            if not AWS_AVAILABLE:
                self.s3_status = "boto3 not available"
            elif not S3_ENABLED:
                self.s3_status = "S3 disabled in config"

    def _test_s3_access(self):
        """Test S3 access."""
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            self.s3_status = "Ready - Bucket Accessible"
            logger.info(f"✅ S3 bucket {S3_BUCKET_NAME} is accessible")
        except ClientError as e:  # type: ignore
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            self.s3_status = f"S3 Error: {error_code}"
            logger.error(f"❌ S3 error: {e}")

    def generate_presigned_upload_url(self, filename, content_type, max_size_mb=2048):
        """Generate presigned URL for direct S3 upload."""
        if not self.s3_client:
            return None, "S3 client not available"
        try:
            timestamp = datetime.now().strftime('%Y/%m/%d')
            import uuid
            unique_id = str(uuid.uuid4())
            file_key = f"{S3_RAW_UPLOADS_PREFIX}{timestamp}/{unique_id}_{filename}"
            conditions = [
                ["content-length-range", 1, max_size_mb * 1024 * 1024],
                {"Content-Type": content_type}
            ]
            response = self.s3_client.generate_presigned_post(
                Bucket=S3_BUCKET_NAME,
                Key=file_key,
                Fields={"Content-Type": content_type},
                Conditions=conditions,
                ExpiresIn=3600
            )
            logger.info(f"✅ Generated presigned URL for {filename}: {file_key}")
            return {
                'url': response['url'],
                'fields': response['fields'],
                'key': file_key
            }, None
        except Exception as e:
            logger.error(f"❌ Error generating presigned URL: {e}")
            return None, f"Upload URL generation failed: {str(e)}"

    def read_file_from_s3(self, s3_key):
        """Read file content from S3 (for uploads only)."""
        if not self.s3_client:
            return None, "S3 client not available"
        try:
            logger.info(f"📖 Reading file from S3: {s3_key}")
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            content = response['Body'].read()
            logger.info(f"✅ Successfully read {len(content)} bytes from S3: {s3_key}")
            return content, None
        except ClientError as e:  # type: ignore
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                return None, "File not found"
            logger.error(f"❌ Error reading from S3: {e}")
            return None, f"Read error: {str(e)}"
        except Exception as e:
            logger.error(f"❌ Error reading from S3: {e}")
            return None, f"Unexpected read error: {str(e)}"

    def write_file_to_s3(self, s3_key, content, content_type='text/csv'):
        """Write file content to S3."""
        if not self.s3_client:
            return False, "S3 client not available"
        try:
            if isinstance(content, str):
                content = content.encode('utf-8')
            logger.info(f"📝 Writing {len(content)} bytes to S3: {s3_key}")
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=content,
                ContentType=content_type
            )
            logger.info(f"✅ Successfully wrote file to S3: {s3_key}")
            return True, None
        except Exception as e:
            logger.error(f"❌ Error writing to S3: {e}")
            return False, f"Write error: {str(e)}"

    def copy_file_in_s3(self, source_key, dest_key):
        """Copy file within S3."""
        if not self.s3_client:
            return False, "S3 client not available"
        try:
            copy_source = {'Bucket': S3_BUCKET_NAME, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=S3_BUCKET_NAME,
                Key=dest_key
            )
            logger.info(f"✅ Copied {source_key} to {dest_key} in S3")
            return True, None
        except Exception as e:
            logger.error(f"❌ Error copying in S3: {e}")
            return False, f"Copy error: {str(e)}"

    def delete_file(self, s3_key):
        """Delete file from S3."""
        if not self.s3_client:
            return False
        try:
            self.s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            logger.info(f"✅ Deleted {s3_key} from S3")
            return True
        except Exception as e:
            logger.error(f"❌ Error deleting from S3: {e}")
            return False

    def file_exists_in_s3(self, s3_key: str) -> bool:
        """Check if file exists in S3."""
        if not self.s3_client:
            return False
        try:
            self.s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                return False
            return False
        except Exception:
            return False

    def upload_text_to_s3(self, text_content, s3_key):
        """Upload text content to S3."""
        if not self.s3_client:
            return False
        try:
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=text_content.encode('utf-8'),
                ContentType='text/plain'
            )
            logger.info(f"Knowledge base uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading text to S3: {e}")
            return False


# ---------------------------------------------------------------------------
# Bedrock helper (Nova Pro via Converse API)
# ---------------------------------------------------------------------------

def _build_bedrock_converse_kwargs(user_message: str, system_prompt: str) -> dict:
    """
    Build kwargs for bedrock-runtime.converse.
    NOTE: We always use modelId here; BEDROCK_MODEL_ID must be a model ID like
          'amazon.nova-pro-v1:0', not an ARN.
    """
    if not BEDROCK_MODEL_ID:
        raise ValueError("BEDROCK_MODEL_ID is not set. Please set it to a model ID.")

    kwargs = {
        "modelId": BEDROCK_MODEL_ID,
        "messages": [
            {
                "role": "user",
                "content": [{"text": user_message}],
            }
        ],
        "inferenceConfig": {
            "maxTokens": 1024,
            "temperature": 0.5,
            "topP": 0.9,
        },
    }

    # System prompt, if provided, using the dedicated 'system' parameter
    if system_prompt:
        kwargs["system"] = [{"text": system_prompt}]

    return kwargs


def call_bedrock_with_context(user_message: str, system_prompt: str) -> str:
    """
    Call AWS Bedrock (Amazon Nova, or any configured model ID) with a system prompt.
    Uses the Bedrock Converse API for text chat.
    """
    try:
        if not magical_agent.bedrock_client:
            return "Bedrock client is not available. Please check AWS configuration."

        kwargs = _build_bedrock_converse_kwargs(user_message, system_prompt)
        response = magical_agent.bedrock_client.converse(**kwargs)

        # Extract the first text block from the assistant's message
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", []) or []

        for block in content_blocks:
            text = block.get("text")
            if text:
                return text

        return "I was unable to generate a response from the model."

    except Exception as e:
        logger.error(f"Bedrock call error: {e}", exc_info=True)
        return f"I encountered an error while calling Bedrock: {str(e)}"


# ---------------------------------------------------------------------------
# Singletons used across the app
# ---------------------------------------------------------------------------

magical_agent = MagicalDataAgent()
s3_manager = S3FileManager()
