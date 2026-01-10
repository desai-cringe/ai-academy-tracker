# application.py  (root)

from app import create_app
from app.aws_utils import magical_agent, s3_manager, S3_BUCKET_NAME, AWS_REGION
from app.services import STATS_CACHE_TTL
import os

app = create_app()
application = app  # for gunicorn (application:application or application:app)

if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("🌟 Starting RDS-Optimized AI Academy Tracker...")
    print("=" * 80)
    print(f"✨ Bedrock Status: {magical_agent.bedrock_status}")
    print(f"📊 S3 Status: {s3_manager.s3_status}")
    print(f"🪣 S3 Bucket: {S3_BUCKET_NAME}")
    print(f"📍 AWS Region: {AWS_REGION}")
    print(f"💾 Database: {os.environ.get('RDS_HOSTNAME', 'localhost')}")
    print(f"🗃️ Database Browser: /dbadmin")
    print("=" * 80)
    print("🛡️ Memory Optimizations:")
    print("   • RDS queries instead of S3 DataFrame loads")
    print("   • Indexed database columns for performance")
    print(f"   • Stats caching: {STATS_CACHE_TTL} seconds")
    print("   • Direct SQL aggregations")
    print("   • Memory-efficient record retrieval")
    print("=" * 80)
    print("🚀 Application starting on http://localhost:5000")
    print("📝 User queries now use RDS (PostgreSQL)")
    print("💾 S3 used for uploads, backups, and storage")
    print("🗃️ Database browser available at /dbadmin")
    print("=" * 80)

    app.run(debug=True)
