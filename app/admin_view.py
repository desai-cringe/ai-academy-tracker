# app/admin_view.py

from flask import session, redirect, url_for, request
from flask_admin.contrib.sqla import ModelView

class SecureModelView(ModelView):
    """Flask-Admin view protected by your session login."""

    # 🔐 This must match your login logic in routes_admin.py
    def is_accessible(self):
        return bool(session.get('logged_in'))

    # If not logged in, send them to /admin/login
    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for('admin_login', next=request.url))

    # --- Config like in your monolithic file ---
    column_display_pk = True
    page_size = 50
    can_create = False   # data comes from uploads
    can_delete = True    # allow cleanup
    can_edit = True      # allow corrections

    column_searchable_list = ['employee_id', 'level', 'issuer', 'skill']
    column_filters = ['level', 'qualifier', 'issuer']
    column_list = [
        "id",
        "assessment_name",
        "employee_id",
        "issuer",
        "level",
        "qualifier",
        "skill",
        "skill_id",
        "valid_till",
        "wipro_function",
    ]
