# app/routes_public.py

import logging
from flask import render_template, request, jsonify
from sqlalchemy import or_
from .models import EmployeeRecord

logger = logging.getLogger(__name__)


def register_public_routes(app):
    @app.route('/')
    def index():
        """Public index page."""
        try:
            return render_template('index.html')
        except Exception as e:
            logger.error(f"❌ Error rendering index: {e}")
            return f"Application error: {str(e)}", 500

    @app.route('/search', methods=['POST'])
    def search():
        """Search by employee ID or email."""
        try:
            # Using the same form field "employee_id" for backward compatibility
            search_value = request.form.get('employee_id', '').strip()
            if not search_value:
                return jsonify({
                    'success': False,
                    'error': 'Please enter an employee ID or email'
                })

            # 🔍 Search by exact employee_id OR email (case-insensitive, partial match)
            records = EmployeeRecord.query.filter(
                or_(
                    EmployeeRecord.employee_id == search_value,
                    EmployeeRecord.email.ilike(f"%{search_value}%")
                )
            ).all()

            if not records:
                return jsonify({
                    'success': False,
                    'error': 'Employee not found by ID or email in our system'
                })

            # Aggregate qualifiers per level
            level_qualifiers = {}
            for record in records:
                level = record.level
                qualifier = record.qualifier
                if level not in level_qualifiers:
                    level_qualifiers[level] = set()
                level_qualifiers[level].add(qualifier)

            # Journey message logic
            message = ""
            if 'Level 3' in level_qualifiers:
                if 'Certified' in level_qualifiers['Level 3']:
                    message = "✅ Completed journey"
                else:
                    message = "⚠️ Complete L3 certification"
            elif 'Level 2' in level_qualifiers:
                if 'Certified' in level_qualifiers['Level 2']:
                    message = "⚠️ Go for L3 training"
                else:
                    message = "⚠️ Complete L2 certification"
            elif 'Level 1' in level_qualifiers:
                if 'Certified' in level_qualifiers['Level 1']:
                    message = "⚠️ Go for L2 training"
                else:
                    message = "⚠️ Complete L1 certification"
            elif 'Level 0' in level_qualifiers:
                if 'Certified' in level_qualifiers['Level 0']:
                    message = "⚠️ Go for L1 training"
                else:
                    message = "⚠️ Complete L0 certification"
            else:
                message = "⚠️ Start with L0 training"

            # Build response records list
            records_list = []
            for record in records:
                records_list.append({
                    'Assessment Name': getattr(record, 'assessment_name', 'N/A'),
                    'Issuer': record.issuer,
                    'Level': record.level,
                    'Qualifier': record.qualifier,
                    'Skill': getattr(record, 'skill', 'N/A'),
                    'Skill_ID': getattr(record, 'skill_id', 'N/A'),
                    'Valid_Till': record.valid_till,
                    'Wipro_Function': record.wipro_function
                })

            return jsonify({
                'success': True,
                # if result came through email search, employee_id may still be present in record
                'employee_id': getattr(records[0], 'employee_id', search_value),
                'message': message,
                'records': records_list
            })
        except Exception as e:
            logger.error(f"Search error: {e}")
            return jsonify({
                'success': False,
                'error': f'System error: {str(e)}'
            })
