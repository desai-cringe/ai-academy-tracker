# app/routes_admin.py

import logging
import gc
import re
import os
import tempfile
from io import BytesIO
import time

import pandas as pd
from flask import (
    render_template, request, jsonify, session, redirect,
    url_for, flash
)
from functools import wraps
from datetime import datetime
from sqlalchemy import func

from .extensions import db
from .models import EmployeeRecord
from .aws_utils import s3_manager, call_bedrock_with_context
from .services import (
    read_employee_data_sample_from_rds,
    get_employee_stats_cached,
    get_chart_data_from_rds,
    write_employee_data_to_s3,
    backup_current_data_in_s3,
    clean_employee_dataframe_chunked,
    create_knowledge_base_file,  # still available if needed elsewhere
    save_cleaned_data_to_rds,
    allowed_file,
    PANDAS_OPTIONS,
    invalidate_stats_cache,
    MAX_DISPLAY_RECORDS,
    REQUIRED_COLUMNS,
    CHUNK_SIZE,
    clear_employee_data_in_rds,
    save_cleaned_data_chunk_to_rds,
    write_employee_csv_string_to_s3,
    create_enhanced_knowledge_base_from_rds,
    get_existing_dedup_keys,
    filter_new_records_for_append,
    export_rds_to_csv_snapshot,
    _deduplicate_cleaned_dataframe,
)

logger = logging.getLogger(__name__)


def register_admin_routes(app):

    # --- Global 500 handler (JSON-safe for AJAX) --- #
    @app.errorhandler(500)
    def handle_internal_error(error):
        # If client expects JSON (e.g., fetch), return JSON instead of HTML
        if request.is_json or request.accept_mimetypes['application/json'] > request.accept_mimetypes['text/html']:
            logger.error(f"Global 500 error on path {request.path}: {error}")
            return jsonify({
                "success": False,
                "error": "Internal server error",
            }), 500
        # Otherwise, you can render a template or keep default
        logger.error(f"Global 500 error (HTML) on path {request.path}: {error}")
        return "Internal Server Error", 500

    # --- login decorator --- #
    def login_required(f):
        """Decorator to require login for admin functions (HTML + JSON safe)."""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                # If it's an AJAX / JSON request, return JSON error instead of HTML
                if request.is_json or request.accept_mimetypes['application/json'] > request.accept_mimetypes['text/html']:
                    return jsonify({
                        'success': False,
                        'error': 'Login required',
                        'login_required': True
                    }), 401
                # Normal browser navigation → redirect to login page
                return redirect(url_for('admin_login'))
            return f(*args, **kwargs)
        return decorated_function

    # --- Auth routes --- #

    @app.route('/admin/login', methods=['GET', 'POST'])
    def admin_login():
        """Admin login."""
        try:
            if request.method == 'POST':
                username = request.form.get('username', '').strip()
                password = request.form.get('password', '').strip()
                logger.info(f"Login attempt - Username: {username}")

                if not username or not password:
                    flash('Please enter both username and password', 'error')
                    return render_template('login.html')

                admin_username = app.config.get('ADMIN_USERNAME', 'admin')
                admin_password = app.config.get('ADMIN_PASSWORD', 'magicalacademy123')

                if username == admin_username and password == admin_password:
                    session['logged_in'] = True
                    session['username'] = username
                    logger.info(f"✅ Successful login: {username}")
                    flash('Successfully logged in!', 'success')
                    return redirect(url_for('admin_panel'))
                else:
                    logger.warning(f"❌ Failed login for user: {username}")
                    flash('Invalid username or password', 'error')
                    return render_template('login.html')

            if session.get('logged_in'):
                return redirect(url_for('admin_panel'))

            return render_template('login.html')
        except Exception as e:
            logger.error(f"❌ Login error: {e}")
            flash('Login system error', 'error')
            return render_template('login.html')

    @app.route('/admin/logout')
    def admin_logout():
        """Admin logout."""
        try:
            session.clear()
            flash('Successfully logged out', 'success')
            return redirect(url_for('admin_login'))
        except Exception as e:
            logger.error(f"❌ Logout error: {e}")
            return redirect(url_for('admin_login'))

    # --- Admin panel and pages --- #

    @app.route('/admin')
    @login_required
    def admin_panel():
        """RDS-optimized admin panel."""
        try:
            logger.info(f"📊 Admin panel accessed by: {session.get('username')}")
            employees = read_employee_data_sample_from_rds()
            if len(employees) == MAX_DISPLAY_RECORDS:
                flash(
                    f'Showing first {MAX_DISPLAY_RECORDS} records for performance. '
                    f'Use search or database browser to find specific employees.',
                    'info'
                )
            stats = get_employee_stats_cached()
            return render_template(
                'admin.html',
                employees=employees,
                username=session.get('username'),
                stats=stats
            )
        except Exception as e:
            logger.error(f"❌ Admin panel error: {e}")
            return f"Admin panel error: {str(e)}", 500

    @app.route('/admin/upload', methods=['GET'])
    @login_required
    def upload_csv():
        """Upload page."""
        try:
            s3_enabled = s3_manager.s3_status.startswith('Ready')
            return render_template(
                'upload.html',
                username=session.get('username'),
                s3_enabled=s3_enabled
            )
        except Exception as e:
            logger.error(f"❌ Upload page error: {e}")
            return f"Upload page error: {str(e)}", 500

    @app.route('/admin/chat')
    @login_required
    def admin_chat():
        """AI Chat page."""
        try:
            return render_template('chat.html', username=session.get('username'))
        except Exception as e:
            logger.error(f"❌ Chat page error: {e}")
            return f"Chat page error: {str(e)}", 500

    # --- Chat API --- #

    @app.route('/admin/chat-api', methods=['POST'])
    @login_required
    def chat_api():
        """Smart chat API with knowledge base + database queries (via Bedrock)."""
        try:
            data = request.get_json()
            user_message = data.get('message', '').strip()
            if not user_message:
                return jsonify({"success": False, "error": "Please enter a message"})

            kb_content, error = s3_manager.read_file_from_s3(
                'knowledge-base/employee_data.txt'
            )
            if error or not kb_content:
                return jsonify({
                    "success": False,
                    "error": "Knowledge base not found. Please refresh it first."
                })
            kb_text = kb_content.decode('utf-8')

            employee_ids = re.findall(r'\b\d{5,}\b', user_message)
            employee_data_text = ""
            if employee_ids:
                for emp_id in employee_ids[:5]:
                    records = EmployeeRecord.query.filter_by(
                        employee_id=emp_id
                    ).all()
                    if records:
                        employee_data_text += f"\n\nEmployee ID {emp_id} Records:\n"
                        for rec in records:
                            employee_data_text += (
                                f"- Level: {rec.level}, Qualifier: {rec.qualifier}, "
                                f"Issuer: {rec.issuer}, Valid Till: {rec.valid_till}\n"
                            )

            system_prompt = f"""You are an AI assistant for the AI Academy Tracker system.

STATISTICAL DATA:
{kb_text}

{f"SPECIFIC EMPLOYEE DATA:{employee_data_text}" if employee_data_text else ""}

Instructions:
- Use the statistical data above for general queries about levels, issuers, distributions
- Use the specific employee data (if provided) for individual employee queries
- If asked about a specific employee not in the data, say you need to query the database
- Provide accurate, helpful answers based on the data provided

Answer the user's question below."""

            response_text = call_bedrock_with_context(user_message, system_prompt)
            return jsonify({
                "success": True,
                "response": response_text,
                # Updated to reflect the AWS-native model
                "model_used": "Amazon Nova Pro (Bedrock)"
            })
        except Exception as e:
            logger.error(f"Chat API error: {e}")
            return jsonify({"success": False, "error": str(e)})

    # --- Change password (placeholder) --- #

    def load_admin_password() -> str:
        """Placeholder – adapt if you had a custom implementation."""
        return app.config.get('ADMIN_PASSWORD', 'magicalacademy123')

    def save_new_password(new_password: str) -> bool:
        """Placeholder – adapt to persist password as you did before."""
        app.config['ADMIN_PASSWORD'] = new_password
        return True

    @app.route('/admin/change-password', methods=['GET', 'POST'])
    @login_required
    def change_password():
        """Change admin password."""
        try:
            if request.method == 'POST':
                current_password = request.form.get('current_password', '').strip()
                new_password = request.form.get('new_password', '').strip()
                confirm_password = request.form.get('confirm_password', '').strip()

                if not all([current_password, new_password, confirm_password]):
                    flash('All password fields are required', 'error')
                    return render_template(
                        'change_password.html',
                        username=session.get('username')
                    )

                stored_password = load_admin_password()

                if current_password != stored_password:
                    flash('Current password is incorrect', 'error')
                elif len(new_password) < 6:
                    flash(
                        'New password must be at least 6 characters long',
                        'error'
                    )
                elif new_password != confirm_password:
                    flash(
                        'New password and confirmation do not match',
                        'error'
                    )
                else:
                    if save_new_password(new_password):
                        flash('Password updated successfully!', 'success')
                        return redirect(url_for('admin_panel'))
                    else:
                        flash('Error updating password. Please try again.', 'error')

            return render_template(
                'change_password.html',
                username=session.get('username')
            )
        except Exception as e:
            logger.error(f"❌ Change password error: {e}")
            return f"Password change service error: {str(e)}", 500

    # --- Upload URL & processing --- #

    @app.route('/admin/upload-url', methods=['POST'])
    @login_required
    def get_upload_url():
        """Generate presigned URL for S3 upload."""
        try:
            data = request.get_json()
            if not data:
                return jsonify({'error': 'Invalid request format'})

            filename = data.get('filename', '').strip()
            content_type = data.get('content_type', '').strip()

            if not filename or not content_type:
                return jsonify({'error': 'Filename and content type are required'})

            if not allowed_file(filename):
                return jsonify({
                    'error': 'Invalid file type. Only CSV and XLSX files are allowed.'
                })

            upload_data, error = s3_manager.generate_presigned_upload_url(
                filename,
                content_type
            )
            if error:
                return jsonify({'error': f'Could not generate upload URL: {error}'})

            return jsonify({
                'success': True,
                'upload_url': upload_data['url'],
                'upload_fields': upload_data['fields'],
                's3_key': upload_data['key']
            })
        except Exception as e:
            logger.error(f"❌ Upload URL error: {e}")
            return jsonify({'error': f'Upload URL service error: {str(e)}'})

    @app.route('/admin/process-upload', methods=['POST'])
    @login_required
    def process_upload():
        """Upload processing with RDS integration (optimized + streaming for CSV)."""
        start_time = time.time()

        def log_step(message: str):
            elapsed = time.time() - start_time
            logger.info(f"[process_upload] [{elapsed:6.2f}s] {message}")

        def detect_csv_encoding(file_bytes):
            """Try a few encodings quickly on a small sample."""
            for encoding in ['utf-8', 'utf-8-sig', 'latin-1']:
                try:
                    _ = pd.read_csv(
                        BytesIO(file_bytes),
                        dtype=PANDAS_OPTIONS['dtype'],
                        low_memory=False,
                        usecols=REQUIRED_COLUMNS,
                        encoding=encoding,
                        nrows=100
                    )
                    return encoding
                except UnicodeDecodeError:
                    continue
                except Exception:
                    # ignore non-encoding errors here
                    continue
            return None

        try:
            log_step("Request received")
            data = request.get_json()
            if not data:
                log_step("Invalid request format (no JSON body)")
                return jsonify({'error': 'Invalid request format'})

            s3_key = data.get('s3_key', '').strip()
            mode = data.get('upload_type', 'replace').strip()

            if not s3_key:
                log_step("Missing S3 key in request")
                return jsonify({'error': 'S3 key is required'})
            if mode not in ['replace', 'append']:
                log_step(f"Invalid upload mode: {mode}")
                return jsonify({'error': 'Invalid upload mode'})

            logger.info(f"📥 Processing upload: {s3_key} (mode: {mode})")
            log_step(f"Validated request: s3_key={s3_key}, mode={mode}")

            # --- 1. Download file from S3 ---
            file_content, error = s3_manager.read_file_from_s3(s3_key)
            if error:
                log_step(f"Error reading file from S3: {error}")
                return jsonify({'error': f'Could not read uploaded file: {error}'})
            log_step("Downloaded file from S3")

            file_extension = s3_key.lower().split('.')[-1]

            # Preload existing dedup keys once for append mode
            existing_keys = get_existing_dedup_keys() if mode == 'append' else set()
            if mode == 'append':
                log_step(f"Loaded {len(existing_keys)} existing dedup keys for append mode")

            # --- 2. Backup existing S3 snapshot BEFORE processing ---
            backup_key = backup_current_data_in_s3(mode)
            log_step(f"Backup completed (key={backup_key})")

            # --- XLSX path: non-streaming but optimized --- #
            if file_extension == 'xlsx':
                try:
                    df = pd.read_excel(
                        BytesIO(file_content),
                        engine='openpyxl',
                        usecols=REQUIRED_COLUMNS
                    )
                    original_rows = len(df)
                    log_step(f"Parsed XLSX into DataFrame with {original_rows} rows")
                except Exception as read_error:
                    logger.error(f"❌ Error reading XLSX file: {read_error}")
                    s3_manager.delete_file(s3_key)
                    log_step(f"Error while parsing uploaded XLSX file: {read_error}")
                    return jsonify({'error': 'Invalid file format or corrupted file'})

                if df is None or df.empty:
                    s3_manager.delete_file(s3_key)
                    log_step("Uploaded XLSX contained no data (empty DataFrame)")
                    return jsonify({'error': 'Uploaded file contains no data'})

                try:
                    df_cleaned = clean_employee_dataframe_chunked(df)
                    if df_cleaned is None or df_cleaned.empty:
                        s3_manager.delete_file(s3_key)
                        log_step("Cleaning XLSX left no valid rows")
                        return jsonify({
                            'error': 'No valid data remains after cleaning'
                        })
                    cleaned_rows = len(df_cleaned)
                    dropped_rows = original_rows - cleaned_rows
                    log_step(
                        f"Cleaned XLSX DataFrame: {cleaned_rows} rows "
                        f"(dropped {dropped_rows} invalid rows)"
                    )
                except Exception as cleaning_error:
                    logger.error(f"❌ Error cleaning XLSX data: {cleaning_error}")
                    s3_manager.delete_file(s3_key)
                    log_step(f"Data cleaning failed: {cleaning_error}")
                    return jsonify({
                        'error': f'Data cleaning failed: {str(cleaning_error)}'
                    })

                missing_columns = [
                    col for col in REQUIRED_COLUMNS if col not in df_cleaned.columns
                ]
                if missing_columns:
                    s3_manager.delete_file(s3_key)
                    log_step(f"Missing required columns: {', '.join(missing_columns)}")
                    return jsonify({
                        'error': f'Missing required columns: {", ".join(missing_columns)}'
                    })

                dedup_keys_in_file: set[str] = set()
                df_cleaned, duplicates_in_file, dedup_keys_in_file = _deduplicate_cleaned_dataframe(
                    df_cleaned,
                    dedup_keys_in_file
                )
                if df_cleaned is None or df_cleaned.empty:
                    s3_manager.delete_file(s3_key)
                    log_step("Uploaded XLSX contained only duplicate rows after deduplication")
                    return jsonify({
                        'error': 'Uploaded file contains only duplicate rows'
                    })

                cleaned_rows = int(len(df_cleaned))
                dropped_rows = original_rows - cleaned_rows
                duplicates_skipped = 0
                duplicates_total = int(duplicates_in_file)
                s3_error = None

                if mode == 'append':
                    df_for_db, duplicates_skipped = filter_new_records_for_append(df_cleaned, existing_keys)
                    duplicates_skipped = int(duplicates_skipped)
                    duplicates_total = int(duplicates_in_file + duplicates_skipped)
                    cleaned_rows = int(len(df_for_db))
                    dropped_rows = original_rows - cleaned_rows
                    log_step(
                        f"Append dedup complete: {duplicates_total} duplicates skipped, "
                        f"{cleaned_rows} new rows to insert"
                    )

                    if cleaned_rows == 0:
                        s3_manager.delete_file(s3_key)
                        invalidate_stats_cache()
                        log_step("No new rows to append after deduplication")
                        return jsonify({
                            'success': True,
                            'message': 'No new records to append; all rows were duplicates.',
                            'stats': {
                                'mode': 'append',
                                'count': 0,
                                'added': 0,
                                'dropped': int(dropped_rows),
                                'invalid': int(dropped_rows),
                                'duplicates': duplicates_total,
                                'skipped': duplicates_total,
                                'total': int(dropped_rows + duplicates_total),
                                'rds_success': True,
                                's3_success': True
                            }
                        })

                    rds_result, rds_error = save_cleaned_data_chunk_to_rds(
                        df_for_db,
                        already_mapped=True
                    )
                    log_step(
                        f"save_cleaned_data_chunk_to_rds completed "
                        f"(success={rds_result}, error={rds_error})"
                    )

                    if rds_result:
                        s3_result, s3_error = export_rds_to_csv_snapshot()
                        log_step(
                            f"export_rds_to_csv_snapshot completed "
                            f"(success={s3_result}, error={s3_error})"
                        )
                    else:
                        s3_result = False
                else:
                    s3_result = write_employee_data_to_s3(df_cleaned)
                    log_step(f"write_employee_data_to_s3 completed (success={s3_result})")

                    rds_result, rds_error = save_cleaned_data_to_rds(df_cleaned)
                    log_step(
                        f"save_cleaned_data_to_rds completed "
                        f"(success={rds_result}, error={rds_error})"
                    )

                # Build enhanced KB from RDS
                kb_content, record_count, issuer_count, kb_error = create_enhanced_knowledge_base_from_rds()
                if kb_content and not kb_error:
                    kb_uploaded = s3_manager.upload_text_to_s3(
                        kb_content,
                        'knowledge-base/employee_data.txt'
                    )
                    if kb_uploaded:
                        logger.info("Knowledge base (enhanced) created successfully from RDS (XLSX path)")
                        log_step("KB created & uploaded to S3 (XLSX path)")
                    else:
                        log_step("KB generation OK, but S3 upload failed (XLSX path)")
                else:
                    log_step(f"KB generation from RDS failed (XLSX path): {kb_error}")

                if s3_result and rds_result:
                    if mode == 'append':
                        success_msg = (
                            f'Data appended successfully! {cleaned_rows} new records added '
                            f'(duplicates skipped: {duplicates_total})'
                        )
                    else:
                        success_msg = (
                            f'Data replaced successfully! {cleaned_rows} records loaded '
                            f'(S3 backup and RDS updated)'
                        )
                elif rds_result:
                    success_msg = (
                        'Data saved in RDS successfully, but S3 backup failed'
                    )
                elif s3_result:
                    success_msg = (
                        f'Data saved in S3, but database update failed: {rds_error or s3_error}'
                    )
                else:
                    s3_manager.delete_file(s3_key)
                    raise Exception("Failed to write data to both S3 and RDS")

                if dropped_rows > 0:
                    success_msg += f', {dropped_rows} invalid records cleaned'
                if mode == 'append' and duplicates_total > 0:
                    success_msg += f', {duplicates_total} duplicates skipped'
                elif mode == 'replace' and duplicates_total > 0:
                    success_msg += f', {duplicates_total} duplicate rows removed'

                result_stats = {
                    'mode': mode,
                    'count': int(cleaned_rows),
                    'added': int(cleaned_rows),
                    'dropped': int(dropped_rows),
                    'invalid': int(dropped_rows),
                    'duplicates': int(duplicates_total),
                    'skipped': int(duplicates_total),
                    'total': int(cleaned_rows + duplicates_total + max(dropped_rows, 0)),
                    'rds_success': bool(rds_result),
                    's3_success': bool(s3_result)
                }

                s3_manager.delete_file(s3_key)
                invalidate_stats_cache()
                del df, df_cleaned, file_content
                gc.collect()
                log_step("Cleanup complete and stats cache invalidated (XLSX)")

                logger.info(f"✅ Upload (XLSX) processed successfully: {success_msg}")
                log_step("Finished process_upload for XLSX successfully (returning 200)")
                return jsonify({
                    'success': True,
                    'message': success_msg,
                    'stats': result_stats
                })

            # --- CSV path: streaming in chunks --- #
            encoding = detect_csv_encoding(file_content)
            if not encoding:
                s3_manager.delete_file(s3_key)
                log_step("Failed to detect CSV encoding")
                return jsonify({'error': 'Could not decode CSV file'})

            log_step(f"Detected CSV encoding: {encoding}")

            if mode == 'replace':
                # Clear RDS table once for replace mode
                clear_ok, clear_error = clear_employee_data_in_rds()
                if not clear_ok:
                    s3_manager.delete_file(s3_key)
                    log_step(f"Failed to clear RDS table: {clear_error}")
                    return jsonify({
                        'error': f'Upload processing failed while clearing database: {clear_error}'
                    })

                total_original_rows = 0
                total_cleaned_rows = 0
                duplicates_in_file = 0
                required_checked = False
                dedup_keys_in_file: set[str] = set()

                # Temp file for building CSV for S3 snapshot (streaming)
                temp_file = tempfile.NamedTemporaryFile(
                    mode='w+', newline='', suffix='.csv', delete=False, encoding='utf-8'
                )
                temp_file_path = temp_file.name
                wrote_header = False

                try:
                    csv_stream = BytesIO(file_content)
                    for chunk in pd.read_csv(
                        csv_stream,
                        dtype=PANDAS_OPTIONS['dtype'],
                        low_memory=False,
                        usecols=REQUIRED_COLUMNS,
                        encoding=encoding,
                        chunksize=CHUNK_SIZE
                    ):
                        chunk_original_rows = len(chunk)
                        total_original_rows += chunk_original_rows

                        cleaned_chunk = clean_employee_dataframe_chunked(chunk)
                        if cleaned_chunk is None or cleaned_chunk.empty:
                            continue

                        if not required_checked:
                            missing_columns = [
                                col for col in REQUIRED_COLUMNS if col not in cleaned_chunk.columns
                            ]
                            if missing_columns:
                                s3_manager.delete_file(s3_key)
                                log_step(f"Missing required columns in first chunk: {', '.join(missing_columns)}")
                                temp_file.close()
                                os.unlink(temp_file_path)
                                return jsonify({
                                    'error': f'Missing required columns: {", ".join(missing_columns)}'
                                })
                            required_checked = True

                        cleaned_chunk, chunk_duplicates, dedup_keys_in_file = _deduplicate_cleaned_dataframe(
                            cleaned_chunk,
                            dedup_keys_in_file
                        )
                        duplicates_in_file += chunk_duplicates
                        if cleaned_chunk is None or cleaned_chunk.empty:
                            continue

                        chunk_cleaned_rows = len(cleaned_chunk)
                        total_cleaned_rows += chunk_cleaned_rows

                        # Append to RDS
                        rds_result, rds_error = save_cleaned_data_chunk_to_rds(cleaned_chunk)
                        if not rds_result:
                            s3_manager.delete_file(s3_key)
                            log_step(f"RDS chunk save failed: {rds_error}")
                            temp_file.close()
                            os.unlink(temp_file_path)
                            return jsonify({
                                'error': f'Upload processing failed while saving to database: {rds_error}'
                            })

                        # Append to temp CSV for S3 snapshot
                        cleaned_chunk.to_csv(
                            temp_file,
                            index=False,
                            header=not wrote_header,
                            encoding='utf-8'
                        )
                        wrote_header = True

                    temp_file.flush()

                    if total_cleaned_rows == 0:
                        s3_manager.delete_file(s3_key)
                        log_step("No valid data in any CSV chunk after cleaning")
                        temp_file.close()
                        os.unlink(temp_file_path)
                        return jsonify({
                            'error': 'No valid data remains after cleaning'
                        })

                    dropped_rows = total_original_rows - total_cleaned_rows
                    log_step(
                        f"Finished CSV chunk processing: {total_cleaned_rows} cleaned rows "
                        f"(dropped {dropped_rows})"
                    )

                    # --- Upload combined CSV from temp file to S3 snapshot --- #
                    temp_file.seek(0)
                    csv_content = temp_file.read()
                    temp_file.close()
                    s3_result = write_employee_csv_string_to_s3(csv_content)
                    log_step(f"write_employee_csv_string_to_s3 completed (success={s3_result})")

                    # Build enhanced KB from RDS
                    kb_content, record_count, issuer_count, kb_error = create_enhanced_knowledge_base_from_rds()
                    if kb_content and not kb_error:
                        kb_uploaded = s3_manager.upload_text_to_s3(
                            kb_content,
                            'knowledge-base/employee_data.txt'
                        )
                        if kb_uploaded:
                            logger.info("Knowledge base (enhanced) created successfully from RDS (CSV path)")
                            log_step("KB created & uploaded to S3 (CSV path)")
                        else:
                            log_step("KB generation OK, but S3 upload failed (CSV path)")
                    else:
                        log_step(f"KB generation from RDS failed (CSV path): {kb_error}")

                    if not s3_result:
                        success_msg = (
                            'Data replaced in RDS successfully, but S3 backup failed'
                        )
                    else:
                        success_msg = (
                            f'Data replaced successfully! {total_cleaned_rows} records loaded '
                            f'(S3 backup and RDS updated)'
                        )

                    if dropped_rows > 0:
                        success_msg += f', {dropped_rows} invalid records cleaned'
                    if duplicates_in_file > 0:
                        success_msg += f', {duplicates_in_file} duplicate rows removed'

                    result_stats = {
                        'mode': 'replace',
                        'count': int(total_cleaned_rows),
                        'added': int(total_cleaned_rows),
                        'dropped': int(dropped_rows),
                        'invalid': int(dropped_rows),
                        'duplicates': int(duplicates_in_file),
                        'skipped': int(duplicates_in_file),
                        'total': int(total_cleaned_rows + duplicates_in_file + max(dropped_rows, 0)),
                        'rds_success': True,
                        's3_success': bool(s3_result)
                    }

                    # Cleanup
                    s3_manager.delete_file(s3_key)
                    invalidate_stats_cache()
                    del file_content
                    gc.collect()
                    os.unlink(temp_file_path)
                    log_step("Cleanup complete and stats cache invalidated (CSV)")

                    logger.info(f"✅ Upload (CSV) processed successfully: {success_msg}")
                    log_step("Finished process_upload for CSV successfully (returning 200)")
                    return jsonify({
                        'success': True,
                        'message': success_msg,
                        'stats': result_stats
                    })
                except Exception as processing_error:
                    try:
                        temp_file.close()
                        if os.path.exists(temp_file_path):
                            os.unlink(temp_file_path)
                    except Exception:
                        pass
                    s3_manager.delete_file(s3_key)
                    logger.error(f"❌ Processing error (CSV): {processing_error}")
                    log_step(f"Processing error occurred (CSV): {processing_error}")
                    return jsonify({
                        'error': f'Upload processing failed: {str(processing_error)}'
                    })
            else:
                total_original_rows = 0
                total_cleaned_rows = 0
                total_duplicates = 0
                required_checked = False

                try:
                    csv_stream = BytesIO(file_content)
                    for chunk in pd.read_csv(
                        csv_stream,
                        dtype=PANDAS_OPTIONS['dtype'],
                        low_memory=False,
                        usecols=REQUIRED_COLUMNS,
                        encoding=encoding,
                        chunksize=CHUNK_SIZE
                    ):
                        chunk_original_rows = len(chunk)
                        total_original_rows += chunk_original_rows

                        cleaned_chunk = clean_employee_dataframe_chunked(chunk)
                        if cleaned_chunk is None or cleaned_chunk.empty:
                            continue

                        if not required_checked:
                            missing_columns = [
                                col for col in REQUIRED_COLUMNS if col not in cleaned_chunk.columns
                            ]
                            if missing_columns:
                                s3_manager.delete_file(s3_key)
                                log_step(f"Missing required columns in first chunk: {', '.join(missing_columns)}")
                                return jsonify({
                                    'error': f'Missing required columns: {", ".join(missing_columns)}'
                                })
                            required_checked = True

                        deduped_chunk, dupes = filter_new_records_for_append(cleaned_chunk, existing_keys)
                        if deduped_chunk is None or deduped_chunk.empty:
                            total_duplicates += dupes
                            continue

                        chunk_cleaned_rows = len(deduped_chunk)
                        total_cleaned_rows += chunk_cleaned_rows
                        total_duplicates += dupes

                        rds_result, rds_error = save_cleaned_data_chunk_to_rds(
                            deduped_chunk,
                            already_mapped=True
                        )
                        if not rds_result:
                            s3_manager.delete_file(s3_key)
                            log_step(f"RDS chunk save failed (append): {rds_error}")
                            return jsonify({
                                'error': f'Upload processing failed while saving to database: {rds_error}'
                            })

                    if total_cleaned_rows == 0:
                        s3_manager.delete_file(s3_key)
                        log_step("No new rows to append after deduplication")
                        return jsonify({
                            'success': True,
                            'message': 'No new records to append; all rows were duplicates.',
                            'stats': {
                                'mode': 'append',
                                'count': 0,
                                'added': 0,
                                'dropped': int(total_original_rows),
                                'invalid': int(total_original_rows),
                                'duplicates': int(total_duplicates),
                                'skipped': int(total_duplicates),
                                'total': int(total_original_rows),
                                'rds_success': True,
                                's3_success': True
                            }
                        })

                    dropped_rows = int(total_original_rows - total_cleaned_rows)
                    log_step(
                        f"Finished CSV append processing: {total_cleaned_rows} new rows, "
                        f"{total_duplicates} duplicates skipped"
                    )

                    s3_result, s3_error = export_rds_to_csv_snapshot()
                    log_step(
                        f"export_rds_to_csv_snapshot completed "
                        f"(success={s3_result}, error={s3_error})"
                    )

                    kb_content, record_count, issuer_count, kb_error = create_enhanced_knowledge_base_from_rds()
                    if kb_content and not kb_error:
                        kb_uploaded = s3_manager.upload_text_to_s3(
                            kb_content,
                            'knowledge-base/employee_data.txt'
                        )
                        if kb_uploaded:
                            logger.info("Knowledge base (enhanced) created successfully from RDS (CSV append path)")
                            log_step("KB created & uploaded to S3 (CSV append path)")
                        else:
                            log_step("KB generation OK, but S3 upload failed (CSV append path)")
                    else:
                        log_step(f"KB generation from RDS failed (CSV append path): {kb_error}")

                    success_msg = (
                        f'Data appended successfully! {total_cleaned_rows} new records added '
                        f'(duplicates skipped: {total_duplicates})'
                    )
                    if dropped_rows > 0:
                        success_msg += f', {dropped_rows} invalid records cleaned'

                    result_stats = {
                        'mode': 'append',
                        'count': int(total_cleaned_rows),
                        'added': int(total_cleaned_rows),
                        'dropped': int(dropped_rows),
                        'invalid': int(dropped_rows),
                        'duplicates': int(total_duplicates),
                        'skipped': int(total_duplicates),
                        'total': int(total_cleaned_rows + total_duplicates + max(dropped_rows, 0)),
                        'rds_success': True,
                        's3_success': bool(s3_result)
                    }

                    s3_manager.delete_file(s3_key)
                    invalidate_stats_cache()
                    del file_content
                    gc.collect()
                    log_step("Cleanup complete and stats cache invalidated (CSV append)")

                    logger.info(f"✅ Upload (CSV append) processed successfully: {success_msg}")
                    log_step("Finished process_upload for CSV append successfully (returning 200)")
                    return jsonify({
                        'success': True,
                        'message': success_msg,
                        'stats': result_stats
                    })
                except Exception as processing_error:
                    s3_manager.delete_file(s3_key)
                    logger.error(f"❌ Processing error (CSV append): {processing_error}")
                    log_step(f"Processing error occurred (CSV append): {processing_error}")
                    return jsonify({
                        'error': f'Upload processing failed: {str(processing_error)}'
                    })
        except Exception as e:
            logger.error(f"❌ Upload processing error: {e}")
            log_step(f"Top-level upload processing error: {e}")
            return jsonify({
                'error': f'Upload processing failed: {str(e)}'
            })

    # --- Refresh Knowledge Base from DB (using shared service) --- #

    @app.route('/admin/refresh-knowledge-base', methods=['POST'])
    @login_required
    def refresh_knowledge_base():
        """Create enhanced knowledge base with issuer-level-qualifier breakdown."""
        try:
            logger.info("📚 Creating enhanced knowledge base (manual refresh)...")
            kb_content, record_count, issuer_count, error = create_enhanced_knowledge_base_from_rds()
            if error or not kb_content:
                return jsonify({
                    "success": False,
                    "error": error or "Failed to create knowledge base"
                })

            kb_uploaded = s3_manager.upload_text_to_s3(
                kb_content,
                'knowledge-base/employee_data.txt'
            )
            if kb_uploaded:
                logger.info("✅ Knowledge base uploaded successfully (manual refresh)")
                return jsonify({
                    "success": True,
                    "message": (
                        f"Enhanced knowledge base created with {record_count} records "
                        f"covering {issuer_count} issuers"
                    )
                })
            return jsonify({"success": False, "error": "S3 upload failed"})
        except Exception as e:
            logger.error(f"KB error: {e}")
            return jsonify({"success": False, "error": str(e)})

    # --- Stats & chart APIs --- #

    @app.route('/admin/stats')
    @login_required
    def get_stats():
        """Get cached statistics from RDS."""
        try:
            stats = get_employee_stats_cached()
            return jsonify(stats)
        except Exception as e:
            logger.error(f"❌ Stats error: {e}")
            return jsonify({
                'error': f'Error reading stats: {str(e)}',
                'total_employees': 0,
                'completed_journey': 0,
                'in_progress': 0,
                'all_records': 0,
                'rds_records': 0
            })

    @app.route('/admin/chart-data')
    @login_required
    def chart_data():
        """RDS-optimized chart data."""
        try:
            issuer = request.args.get('issuer', '').strip()
            chart_data_result = get_chart_data_from_rds(issuer)
            return jsonify(chart_data_result)
        except Exception as e:
            logger.error(f"❌ Chart data error: {e}")
            return jsonify({
                'error': f'Error reading chart data: {str(e)}',
                'labels': ['Level 0', 'Level 1', 'Level 2'],
                'values': [0, 0, 0],
                'issuers': []
            })

    @app.route('/admin/certification-data')
    @login_required
    def get_certification_data():
        """Get certification data by level and issuer."""
        try:
            issuer = request.args.get('issuer', '').strip()
            query = db.session.query(
                EmployeeRecord.level,
                EmployeeRecord.qualifier,
                func.count(EmployeeRecord.id)
            )
            if issuer:
                query = query.filter(EmployeeRecord.issuer == issuer)
            results = query.group_by(
                EmployeeRecord.level,
                EmployeeRecord.qualifier
            ).all()
            levels = ['Level 0', 'Level 1', 'Level 2', 'Level 3']
            certified = {level: 0 for level in levels}
            trained = {level: 0 for level in levels}
            for level, qualifier, count in results:
                if level in certified:
                    q = (qualifier or "").lower()
                    if 'certified' in q:
                        certified[level] += count
                    elif 'trained' in q:
                        trained[level] += count
            all_issuers = db.session.query(
                func.distinct(EmployeeRecord.issuer)
            ).filter(
                EmployeeRecord.issuer.isnot(None)
            ).all()
            issuers = sorted([i[0] for i in all_issuers if i[0]])
            return jsonify({
                "labels": levels,
                "certified": [certified[l] for l in levels],
                "trained": [trained[l] for l in levels],
                "issuers": issuers,
                "selected_issuer": issuer
            })
        except Exception as e:
            logger.error(f"Certification data error: {e}")
            return jsonify({"error": str(e)})
