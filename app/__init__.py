# app/__init__.py

import os
import logging
import warnings
from flask import Flask, render_template, jsonify
from datetime import datetime

from .extensions import db, admin
from .models import EmployeeRecord
from .admin_view import SecureModelView
from .aws_utils import magical_agent, s3_manager
from .services import get_employee_stats_cached

# Suppress pandas warnings for cleaner logs
import pandas as pd  # just to ensure pandas warnings config applies
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_app() -> Flask:
    app = Flask(__name__)

    # Security / admin configuration via env vars
    app.secret_key = os.environ.get(
        'SECRET_KEY',
        'your-magical-secret-key-change-this-immediately'
    )
    app.config['ADMIN_USERNAME'] = os.environ.get('ADMIN_USERNAME', 'admin')
    app.config['ADMIN_PASSWORD'] = os.environ.get(
        'ADMIN_PASSWORD', 'magicalacademy123'
    )

    # Large file configuration
    app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB

    # Database configuration from environment
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"postgresql://{os.environ.get('RDS_USERNAME', 'postgres')}:"
        f"{os.environ.get('RDS_PASSWORD', 'AcademyDB2025!')}"
        f"@{os.environ.get('RDS_HOSTNAME', 'localhost')}:"
        f"{os.environ.get('RDS_PORT', '5432')}/"
        f"{os.environ.get('RDS_DB_NAME', 'ai_academy_db')}"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Init extensions
    db.init_app(app)
    admin.init_app(app)   # uses url='/dbadmin' from extensions.py

    # Configure Flask-Admin database browser
    admin.name = '🗃️ Database Browser'
    admin.template_mode = 'bootstrap4'
    admin.add_view(
        SecureModelView(EmployeeRecord, db.session, name='Employee Records')
    )

    # Register routes
    from .routes_public import register_public_routes
    from .routes_admin import register_admin_routes
    register_public_routes(app)
    register_admin_routes(app)

    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        try:
            return render_template('index.html'), 404
        except Exception:
            return "Page not found", 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"❌ 500 error: {error}")
        try:
            return render_template('index.html'), 500
        except Exception:
            return "Internal server error", 500

    @app.errorhandler(413)
    def too_large(error):
        return jsonify({'error': 'File too large. Maximum size is 2GB.'}), 413

    # Health endpoint (matches previous behavior)
    @app.route('/health')
    def health():
        """Health check endpoint"""
        try:
            stats = get_employee_stats_cached()
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                's3_status': s3_manager.s3_status if s3_manager else 'unavailable',
                'bedrock_status': magical_agent.bedrock_status if magical_agent else 'unavailable',
                'rds_available': True,
                'data_available': stats.get('all_records', 0) > 0,
                'record_count': stats.get('all_records', 0),
                'rds_record_count': stats.get('rds_records', 0)
            }
            return jsonify(health_status)
        except Exception as e:
            return jsonify({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    # Create tables
    with app.app_context():
        try:
            db.create_all()
            logger.info("✅ Database tables created successfully")
        except Exception as e:
            logger.error(f"❌ Error creating database tables: {e}")

    return app

