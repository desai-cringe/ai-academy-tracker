# app/extensions.py

from flask_sqlalchemy import SQLAlchemy
from flask_admin import Admin

# Global SQLAlchemy instance (used by models & services)
db = SQLAlchemy()

# Global Flask-Admin instance
# IMPORTANT: set url='/dbadmin' here so the browser lives at /dbadmin
admin = Admin(
    name='🗃️ Database Browser',
    template_mode='bootstrap4',
    url='/dbadmin',
)
