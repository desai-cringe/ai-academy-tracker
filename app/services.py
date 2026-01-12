# app/services.py

import logging
import time
import gc
import os
import tempfile
import csv
from collections import Counter
from datetime import datetime
from io import StringIO
from typing import Optional, Dict, Tuple, List, Set

from dateutil import parser as date_parser

import pandas as pd
from sqlalchemy import func, text

from .extensions import db
from .models import EmployeeRecord
from .aws_utils import s3_manager, S3_CURRENT_DATA_KEY, S3_BACKUP_PREFIX

logger = logging.getLogger(__name__)

# Memory settings
CHUNK_SIZE = 5000  # single source of truth for chunk size
MAX_DISPLAY_RECORDS = 100

# Columns we actually care about for upload/DB/KB
REQUIRED_COLUMNS = [
    'Assessment_ID', 'Assessment Name', 'Name', 'Email',
    'Employee_ID', 'Final Completion Date', 'Issuer', 'Level', 'Marks',
    'Qualifier', 'Skill', 'Skill_ID', 'Valid_Till', 'Wipro_Function'
]

# Columns used to detect duplicates in append mode (DB column names)
APPEND_DEDUP_COLUMNS_DB = [
    'assessment_id', 'assessment_name', 'name', 'email', 'employee_id',
    'final_completion_date', 'issuer', 'level', 'marks', 'qualifier',
    'skill', 'skill_id', 'valid_till', 'wipro_function'
]

# Mapping for exporting data back to CSV (DB column → file column)
CSV_EXPORT_COLUMNS = [
    ('assessment_id', 'Assessment_ID'),
    ('assessment_name', 'Assessment Name'),
    ('name', 'Name'),
    ('email', 'Email'),
    ('employee_id', 'Employee_ID'),
    ('final_completion_date', 'Final Completion Date'),
    ('issuer', 'Issuer'),
    ('level', 'Level'),
    ('marks', 'Marks'),
    ('qualifier', 'Qualifier'),
    ('skill', 'Skill'),
    ('skill_id', 'Skill_ID'),
    ('valid_till', 'Valid_Till'),
    ('wipro_function', 'Wipro_Function'),
]

# Pandas options and dtypes
PANDAS_OPTIONS = {
    'dtype': {
        'Assessment_ID': 'string',
        'Assessment Name': 'string',
        'Name': 'string',
        'Email': 'string',
        'Employee_ID': 'string',
        'Final Completion Date': 'string',
        'Issuer': 'string',
        'Level': 'string',
        'Marks': 'string',
        'Qualifier': 'string',
        'Skill': 'string',
        'Skill_ID': 'string',
        'Valid_Till': 'string',
        'Wipro_Function': 'string'
    }
}

# Stats cache
STATS_CACHE_TTL = 300  # 5 minutes
_stats_cache = {'data': None, 'timestamp': None, 'ttl': STATS_CACHE_TTL}


def invalidate_stats_cache():
    _stats_cache['data'] = None
    _stats_cache['timestamp'] = None


# --- DB IO helpers --- #


def _map_df_to_db_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns to match EmployeeRecord model fields."""
    return df.rename(columns={
        'Assessment_ID': 'assessment_id',
        'Assessment Name': 'assessment_name',
        'Name': 'name',
        'Email': 'email',
        'Employee_ID': 'employee_id',
        'Final Completion Date': 'final_completion_date',
        'Issuer': 'issuer',
        'Level': 'level',
        'Marks': 'marks',
        'Qualifier': 'qualifier',
        'Skill': 'skill',
        'Skill_ID': 'skill_id',
        'Valid_Till': 'valid_till',
        'Wipro_Function': 'wipro_function'
    })


def _build_dedup_key_from_row(values: Dict[str, str]) -> str:
    """Create a normalized key across all dedup columns."""
    normalized_parts = [
        str(values.get(col, '') or '').strip().lower() for col in APPEND_DEDUP_COLUMNS_DB
    ]
    return "|".join(normalized_parts)


def _deduplicate_cleaned_dataframe(
    df: pd.DataFrame,
    seen_keys: Optional[Set[str]] = None
) -> Tuple[pd.DataFrame, int, Set[str]]:
    """
    Remove duplicate rows from a cleaned DataFrame using the standard dedup key.
    Returns (deduped_df, duplicate_count, updated_seen_keys).
    """
    if seen_keys is None:
        seen_keys = set()

    if df is None or df.empty:
        return pd.DataFrame(), 0, seen_keys

    df_for_keys = _map_df_to_db_columns(df).fillna("")
    df_for_keys['__dedup_key'] = df_for_keys.apply(_build_dedup_key_from_row, axis=1)

    already_seen_mask = df_for_keys['__dedup_key'].isin(seen_keys)
    duplicate_within_chunk_mask = df_for_keys['__dedup_key'].duplicated()
    unique_mask = ~(already_seen_mask | duplicate_within_chunk_mask)

    duplicate_count = int((already_seen_mask | duplicate_within_chunk_mask).sum())
    deduped_df = df.loc[unique_mask.values].copy()
    seen_keys.update(df_for_keys.loc[unique_mask, '__dedup_key'].astype(str))

    return deduped_df, duplicate_count, seen_keys


def get_existing_dedup_keys(batch_size: int = CHUNK_SIZE) -> Set[str]:
    """Return a set of existing dedup keys from the database for append mode."""
    keys: Set[str] = set()
    try:
        query = db.session.query(EmployeeRecord)
        for record in query.yield_per(batch_size):
            record_dict = {col: getattr(record, col, '') for col in APPEND_DEDUP_COLUMNS_DB}
            keys.add(_build_dedup_key_from_row(record_dict))
    except Exception as e:
        logger.error(f"❌ Error building existing dedup keys: {e}")
    return keys


def filter_new_records_for_append(df_chunk: pd.DataFrame, existing_keys: Set[str]) -> Tuple[pd.DataFrame, int]:
    """
    Remove duplicates from a cleaned chunk using existing dedup keys.
    Returns (df_mapped_for_db, duplicate_count).
    """
    if df_chunk is None or df_chunk.empty:
        return pd.DataFrame(), 0

    df_for_db = _map_df_to_db_columns(df_chunk).fillna("")
    df_for_db['__dedup_key'] = df_for_db.apply(_build_dedup_key_from_row, axis=1)

    dedup_keys_series = df_for_db['__dedup_key']
    existing_mask = dedup_keys_series.isin(existing_keys)
    duplicate_within_chunk_mask = dedup_keys_series.duplicated()
    unique_mask = ~(existing_mask | duplicate_within_chunk_mask)
    unique_count = int(unique_mask.sum())
    duplicate_count = int(len(df_for_db) - unique_count)

    new_records = df_for_db[unique_mask.values].copy()
    new_keys = set(new_records['__dedup_key'].tolist())
    existing_keys.update(new_keys)

    new_records.drop(columns=['__dedup_key'], inplace=True)
    return new_records, duplicate_count


def export_rds_to_csv_snapshot() -> Tuple[bool, Optional[str]]:
    """Export entire employee_records table to CSV string for S3 snapshot."""
    try:
        temp_file = tempfile.NamedTemporaryFile(mode='w+', newline='', suffix='.csv', delete=False, encoding='utf-8')
        temp_path = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow([col[1] for col in CSV_EXPORT_COLUMNS])

        query = db.session.query(EmployeeRecord)
        row_count = 0
        for record in query.yield_per(CHUNK_SIZE):
            row = [getattr(record, col, '') or '' for col, _ in CSV_EXPORT_COLUMNS]
            writer.writerow(row)
            row_count += 1

        temp_file.flush()
        temp_file.seek(0)
        csv_content = temp_file.read()
        temp_file.close()

        success, error = s3_manager.write_file_to_s3(S3_CURRENT_DATA_KEY, csv_content)
        os.unlink(temp_path)

        if success:
            logger.info(f"✅ Exported {row_count} records to S3 snapshot")
            return True, None
        logger.error(f"❌ Failed to export CSV snapshot: {error}")
        return False, error
    except Exception as e:
        logger.error(f"❌ Error exporting CSV snapshot: {e}")
        return False, str(e)

def clear_employee_data_in_rds() -> Tuple[bool, Optional[str]]:
    """Truncate employee_records table (for replace mode, streaming path)."""
    try:
        logger.info("🧹 Truncating employee_records table (clear_employee_data_in_rds)...")
        db.session.execute(text("TRUNCATE TABLE employee_records RESTART IDENTITY"))
        db.session.commit()
        return True, None
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Error truncating employee_records: {e}")
        return False, f"Database error during truncate: {str(e)}"


def save_cleaned_data_chunk_to_rds(
    df_chunk: pd.DataFrame,
    *,
    already_mapped: bool = False
) -> Tuple[bool, Optional[str]]:
    """Append a cleaned chunk of data into RDS (no truncate)."""
    if df_chunk is None or df_chunk.empty:
        return True, None  # nothing to do

    try:
        if already_mapped:
            df_for_db = df_chunk
        else:
            df_for_db = _map_df_to_db_columns(df_chunk)
        records = df_for_db.fillna("").to_dict(orient='records')

        db.session.bulk_insert_mappings(EmployeeRecord, records)
        db.session.commit()
        logger.info(f"🚚 Inserted chunk of {len(records)} records into RDS")
        return True, None
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Error saving chunk to RDS: {e}")
        return False, f"Database error during chunk insert: {str(e)}"


def save_cleaned_data_to_rds(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """Save cleaned data to RDS PostgreSQL database (replace mode, non-stream path)."""
    if df is None or df.empty:
        return False, "DataFrame is empty"

    try:
        total = len(df)
        logger.info(f"💾 Saving {total} records to RDS (optimized replace)...")

        # --- 1) TRUNCATE instead of DELETE to avoid massive dead tuples --- #
        logger.info("🧹 Truncating employee_records table (save_cleaned_data_to_rds)...")
        db.session.execute(text("TRUNCATE TABLE employee_records RESTART IDENTITY"))
        db.session.commit()

        # --- 2) Prepare records for bulk insert --- #
        df_for_db = _map_df_to_db_columns(df)
        records = df_for_db.fillna("").to_dict(orient='records')

        # --- 3) Chunked bulk insert inside a single transaction --- #
        inserted = 0
        logger.info(f"🚚 Bulk inserting in chunks of {CHUNK_SIZE}...")

        with db.session.begin():
            for start in range(0, len(records), CHUNK_SIZE):
                end = start + CHUNK_SIZE
                chunk = records[start:end]

                db.session.bulk_insert_mappings(EmployeeRecord, chunk)
                inserted += len(chunk)

                if inserted % (CHUNK_SIZE * 10) == 0 or inserted == total:
                    logger.info(f"   → inserted {inserted}/{total} records into RDS")

        logger.info(f"✅ Successfully saved {inserted} records to RDS (replace mode)")
        return True, None

    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Error saving to RDS: {e}")
        return False, f"Database error: {str(e)}"


def get_rds_record_count() -> int:
    try:
        return db.session.query(EmployeeRecord).count()
    except Exception as e:
        logger.error(f"❌ Error getting RDS record count: {e}")
        return 0


def read_employee_data_sample_from_rds(limit: int = MAX_DISPLAY_RECORDS) -> List[Dict]:
    """Read a sample of employee data from RDS for display in admin panel."""
    try:
        logger.info(f"📖 Reading {limit} records from RDS for display")
        records = EmployeeRecord.query.limit(limit).all()
        result = []
        for record in records:
            result.append({
                'Assessment Name': record.assessment_name or 'N/A',
                'Employee_ID': record.employee_id or 'N/A',
                'Issuer': record.issuer or 'N/A',
                'Level': record.level or 'N/A',
                'Qualifier': record.qualifier or 'N/A',
                'Skill': record.skill or 'N/A',
                'Skill_ID': record.skill_id or 'N/A',
                'Valid_Till': record.valid_till or 'N/A',
                'Wipro_Function': record.wipro_function or 'N/A'
            })
        logger.info(f"✅ Successfully read {len(result)} records from RDS")
        return result
    except Exception as e:
        logger.error(f"❌ Error reading sample data from RDS: {e}")
        return []


# --- Stats & chart data --- #


def get_employee_stats_from_rds() -> Dict[str, int]:
    """Get employee statistics from RDS using SQL aggregations."""
    try:
        logger.info("📊 Calculating stats from RDS...")
        all_records = db.session.query(EmployeeRecord).count()
        if all_records == 0:
            return {
                'total_employees': 0,
                'completed_journey': 0,
                'in_progress': 0,
                'not_started': 0,
                'all_records': 0,
                'rds_records': 0,
                'last_updated': 'No data'
            }

        unique_employees = db.session.query(
            func.count(func.distinct(EmployeeRecord.employee_id))
        ).scalar() or 0

        completed_journey = 0
        in_progress = 0
        not_started = 0

        try:
            result = db.session.execute(text("""
                SELECT employee_id, MAX(
                    CASE 
                        WHEN level LIKE '%Level 2%' THEN 2
                        WHEN level LIKE '%Level 1%' THEN 1
                        WHEN level LIKE '%Level 0%' THEN 0
                        ELSE -1
                    END
                ) as max_level
                FROM employee_records 
                WHERE employee_id IS NOT NULL AND employee_id != ''
                GROUP BY employee_id
            """)).fetchall()

            for row in result:
                max_level = row[1]
                if max_level >= 2:
                    completed_journey += 1
                elif max_level >= 0:
                    in_progress += 1
                else:
                    not_started += 1
        except Exception as level_error:
            logger.warning(f"⚠️ Error in level analysis, using fallback: {level_error}")
            completed_journey = db.session.query(EmployeeRecord).filter(
                EmployeeRecord.level.like('%Level 2%')
            ).count()
            in_progress = unique_employees - completed_journey
            not_started = max(unique_employees - completed_journey - in_progress, 0)

        stats = {
            'total_employees': unique_employees,
            'completed_journey': completed_journey,
            'in_progress': in_progress,
            'not_started': not_started,
            'all_records': all_records,
            'rds_records': all_records,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        logger.info(f"📊 RDS Stats calculated: {stats}")
        return stats
    except Exception as e:
        logger.error(f"❌ Error getting RDS stats: {e}")
        return {
            'total_employees': 0,
            'completed_journey': 0,
            'in_progress': 0,
            'not_started': 0,
            'all_records': 0,
            'rds_records': 0,
            'last_updated': 'Error'
        }


def get_employee_stats_cached() -> Dict[str, int]:
    """Get employee statistics with caching using RDS."""
    try:
        now = time.time()
        if (_stats_cache['data'] is not None and
                _stats_cache['timestamp'] is not None and
                now - _stats_cache['timestamp'] < _stats_cache['ttl']):
            return _stats_cache['data']

        stats = get_employee_stats_from_rds()
        _stats_cache['data'] = stats
        _stats_cache['timestamp'] = now
        return stats
    except Exception as e:
        logger.error(f"❌ Error getting cached employee stats: {e}")
        return get_employee_stats_from_rds()


def get_chart_data_from_rds(issuer_filter: str = "") -> Dict:
    """Get chart data from RDS for level distribution."""
    try:
        logger.info(f"📊 Getting chart data from RDS (issuer: {issuer_filter})")
        query = db.session.query(EmployeeRecord)
        if issuer_filter:
            query = query.filter(EmployeeRecord.issuer == issuer_filter)
        records = query.all()

        if not records:
            return {
                'labels': ['Level 0', 'Level 1', 'Level 2'],
                'values': [0, 0, 0],
                'issuers': []
            }

        level_counts = {'Level 0': 0, 'Level 1': 0, 'Level 2': 0}
        for record in records:
            level = record.level or ''
            if 'Level 2' in level:
                level_counts['Level 2'] += 1
            elif 'Level 1' in level:
                level_counts['Level 1'] += 1
            elif 'Level 0' in level:
                level_counts['Level 0'] += 1

        if not issuer_filter:
            all_issuers = db.session.query(
                func.distinct(EmployeeRecord.issuer)
            ).filter(
                EmployeeRecord.issuer.isnot(None),
                EmployeeRecord.issuer != ''
            ).all()
            issuer_list = sorted([issuer[0] for issuer in all_issuers if issuer[0]])
        else:
            issuer_list = [issuer_filter]

        result = {
            'labels': list(level_counts.keys()),
            'values': list(level_counts.values()),
            'issuers': issuer_list
        }
        logger.info(f"✅ Chart data from RDS: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ Error getting chart data from RDS: {e}")
        return {
            'labels': ['Level 0', 'Level 1', 'Level 2'],
            'values': [0, 0, 0],
            'issuers': []
        }


def get_advanced_insights_data() -> Dict:
    """Build advanced KPI + chart data for executive insights."""
    try:
        level_counts = Counter()
        issuer_counts = Counter()
        skill_counts = Counter()
        qualifier_counts = Counter()
        completion_month_counts = Counter()

        query = db.session.query(
            EmployeeRecord.level,
            EmployeeRecord.issuer,
            EmployeeRecord.skill,
            EmployeeRecord.qualifier,
            EmployeeRecord.final_completion_date,
        )

        for level, issuer, skill, qualifier, completion_date in query.yield_per(CHUNK_SIZE):
            if level:
                normalized_level = level.strip()
                level_counts[normalized_level] += 1
            if issuer:
                issuer_counts[issuer.strip()] += 1
            if skill:
                skill_counts[skill.strip()] += 1
            if qualifier:
                qualifier_counts[qualifier.strip()] += 1
            if completion_date:
                try:
                    parsed_date = date_parser.parse(completion_date)
                    month_key = parsed_date.strftime('%Y-%m')
                    completion_month_counts[month_key] += 1
                except Exception:
                    continue

        top_issuers = issuer_counts.most_common(10)
        top_skills = skill_counts.most_common(10)
        qualifier_breakdown = qualifier_counts.most_common(6)

        months_sorted = sorted(completion_month_counts.keys())
        completion_values = [completion_month_counts[m] for m in months_sorted]
        peak_month = None
        if completion_month_counts:
            peak_month = max(completion_month_counts.items(), key=lambda item: item[1])[0]

        top_issuer_label = top_issuers[0][0] if top_issuers else "N/A"
        top_issuer_value = top_issuers[0][1] if top_issuers else 0
        top_skill_label = top_skills[0][0] if top_skills else "N/A"
        top_skill_value = top_skills[0][1] if top_skills else 0
        top_qualifier_label = qualifier_breakdown[0][0] if qualifier_breakdown else "N/A"
        top_qualifier_value = qualifier_breakdown[0][1] if qualifier_breakdown else 0
        latest_month = months_sorted[-1] if months_sorted else None

        return {
            "level_labels": list(level_counts.keys()) or ["Level 0", "Level 1", "Level 2"],
            "level_values": list(level_counts.values()) or [0, 0, 0],
            "issuer_labels": [item[0] for item in top_issuers],
            "issuer_values": [item[1] for item in top_issuers],
            "skill_labels": [item[0] for item in top_skills],
            "skill_values": [item[1] for item in top_skills],
            "qualifier_labels": [item[0] for item in qualifier_breakdown],
            "qualifier_values": [item[1] for item in qualifier_breakdown],
            "completion_months": months_sorted,
            "completion_values": completion_values,
            "summary": {
                "top_issuer": {"label": top_issuer_label, "value": top_issuer_value},
                "top_skill": {"label": top_skill_label, "value": top_skill_value},
                "top_qualifier": {"label": top_qualifier_label, "value": top_qualifier_value},
                "peak_month": peak_month,
                "latest_month": latest_month,
            },
        }
    except Exception as e:
        logger.error(f"❌ Error building advanced insights: {e}")
        return {
            "level_labels": ["Level 0", "Level 1", "Level 2"],
            "level_values": [0, 0, 0],
            "issuer_labels": [],
            "issuer_values": [],
            "skill_labels": [],
            "skill_values": [],
            "qualifier_labels": [],
            "qualifier_values": [],
            "completion_months": [],
            "completion_values": [],
            "summary": {
                "top_issuer": {"label": "N/A", "value": 0},
                "top_skill": {"label": "N/A", "value": 0},
                "top_qualifier": {"label": "N/A", "value": 0},
                "peak_month": None,
                "latest_month": None,
            },
        }


# --- S3 data write/backup --- #


def write_employee_data_to_s3(df: pd.DataFrame) -> bool:
    """Write employee data DataFrame to S3 for backup/storage (non-stream path)."""
    try:
        if df is None or df.empty:
            logger.error("❌ Cannot write empty DataFrame to S3")
            return False
        logger.info(f"📝 Writing {len(df)} records to S3 for backup...")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        success, error = s3_manager.write_file_to_s3(S3_CURRENT_DATA_KEY, csv_content)
        if success:
            logger.info(f"✅ Successfully wrote {len(df)} records to S3")
            return True
        logger.error(f"❌ Failed to write data to S3: {error}")
        return False
    except Exception as e:
        logger.error(f"❌ Error writing employee data to S3: {e}")
        return False


def write_employee_csv_string_to_s3(csv_content: str) -> bool:
    """Write a pre-built CSV string to S3 (used by streaming upload path)."""
    try:
        if not csv_content:
            logger.error("❌ Cannot write empty CSV string to S3")
            return False
        success, error = s3_manager.write_file_to_s3(S3_CURRENT_DATA_KEY, csv_content)
        if success:
            logger.info("✅ Successfully wrote streamed CSV to S3")
            return True
        logger.error(f"❌ Failed to write streamed CSV to S3: {error}")
        return False
    except Exception as e:
        logger.error(f"❌ Error writing streamed CSV to S3: {e}")
        return False


def backup_current_data_in_s3(mode: str = "replace") -> Optional[str]:
    """Create backup of current data in S3."""
    try:
        if not s3_manager.file_exists_in_s3(S3_CURRENT_DATA_KEY):
            return None
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_key = f"{S3_BACKUP_PREFIX}employee_backup_{mode}_{timestamp}.csv"
        success, error = s3_manager.copy_file_in_s3(S3_CURRENT_DATA_KEY, backup_key)
        if success:
            logger.info(f"✅ Created backup: {backup_key}")
            return backup_key
        logger.error(f"❌ Failed to create backup: {error}")
        return None
    except Exception as e:
        logger.error(f"❌ Error creating backup: {e}")
        return None


# --- Data cleaning & KB --- #


def clean_employee_dataframe_chunked(df: pd.DataFrame) -> pd.DataFrame:
    """Memory-optimized data cleaning."""
    try:
        if df is None or df.empty:
            logger.warning("⚠️ Cannot clean empty DataFrame")
            return pd.DataFrame()
        logger.info(f"🧹 Cleaning DataFrame with {len(df)} records")
        if len(df) > CHUNK_SIZE:
            cleaned_chunks = []
            for i in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[i:i + CHUNK_SIZE].copy()
                cleaned_chunk = _clean_chunk(chunk)
                if not cleaned_chunk.empty:
                    cleaned_chunks.append(cleaned_chunk)
                del chunk
                gc.collect()
            if cleaned_chunks:
                result = pd.concat(cleaned_chunks, ignore_index=True)
                del cleaned_chunks
                gc.collect()
                return result
            return pd.DataFrame()
        else:
            return _clean_chunk(df)
    except Exception as e:
        logger.error(f"❌ Error cleaning dataframe: {e}")
        return df if df is not None else pd.DataFrame()


def _clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Clean a single chunk of data."""
    try:
        required_columns = ['Issuer', 'Level']
        missing_columns = [col for col in required_columns if col not in chunk.columns]
        if missing_columns:
            logger.warning(f"⚠️ Missing columns for cleaning: {missing_columns}")
            return chunk

        chunk['Issuer'] = chunk['Issuer'].astype(str).str.strip()
        chunk['Level'] = chunk['Level'].astype(str).str.strip()

        if 'Employee_ID' in chunk.columns:
            chunk['Employee_ID'] = (
                chunk['Employee_ID']
                .astype(str)
                .str.replace('.0', '', regex=False)
                .str.strip()
            )

        def standardize_level(row):
            try:
                level = str(row['Level']).strip().lower()
                if level == 'level 0':
                    return 'Level 0'
                elif any(x in level for x in ['level 3', 'l3', 'advanced', 'professional', 'competent', 'professional level']):
                    return 'Level 3'
                elif any(x in level for x in ['level 2', 'l2', 'intermediate', 'associate', 'associate level']):
                    return 'Level 2'
                elif any(x in level for x in ['level 1', 'l1', 'foundation', 'foundataional', 'foundational level']):
                    return 'Level 1'
                else:
                    return 'NA'
            except Exception:
                return 'NA'

        chunk['Level'] = chunk.apply(standardize_level, axis=1)
        logger.info(f"🧹 Cleaned chunk: {len(chunk)} records")
        return chunk
    except Exception as e:
        logger.warning(f"⚠️ Error cleaning chunk: {e}")
        return chunk


def create_knowledge_base_file(df_cleaned: pd.DataFrame) -> Optional[str]:
    """Create a simple knowledge base text file from cleaned employee data (DF-based)."""
    try:
        kb_content = []
        kb_content.append("=" * 80)
        kb_content.append("AI ACADEMY TRACKER - EMPLOYEE LEARNING DATA")
        kb_content.append("=" * 80)
        kb_content.append(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        kb_content.append(f"Total Records: {len(df_cleaned)}")
        kb_content.append("")
        kb_content.append("STATISTICS")
        kb_content.append("-" * 80)
        kb_content.append(f"Unique Employees: {df_cleaned['Employee_ID'].nunique()}")
        kb_content.append("")
        kb_content.append("LEVEL DISTRIBUTION:")
        for level, count in df_cleaned['Level'].value_counts().items():
            kb_content.append(f"  {level}: {count} employees")
        kb_content.append("")
        kb_content.append("QUALIFIER DISTRIBUTION:")
        for qual, count in df_cleaned['Qualifier'].value_counts().items():
            kb_content.append(f"  {qual}: {count} records")
        kb_content.append("")
        kb_content.append("SKILL DISTRIBUTION:")
        for skill, count in df_cleaned['Skill'].value_counts().head(15).items():
            kb_content.append(f"  {skill}: {count} records")
        kb_content.append("")
        kb_content.append("CERTIFICATION DISTRIBUTION (ASSESSMENT NAME):")
        for assessment, count in df_cleaned['Assessment Name'].value_counts().head(15).items():
            kb_content.append(f"  {assessment}: {count} records")
        kb_content.append("")
		# ... rest unchanged, kept for backward compatibility ...
        kb_content.append("ISSUER DISTRIBUTION:")
        for issuer, count in df_cleaned['Issuer'].value_counts().items():
            kb_content.append(f"  {issuer}: {count} records")
        kb_content.append("")
        kb_content.append("EMPLOYEE DETAILS")
        kb_content.append("=" * 80)
        kb_content.append("")
        for emp_id in df_cleaned['Employee_ID'].unique():
            emp_records = df_cleaned[df_cleaned['Employee_ID'] == emp_id]
            kb_content.append(f"Employee ID: {emp_id}")
            for _, record in emp_records.iterrows():
                kb_content.append(
                    f"  - Level: {record['Level']}, Qualifier: {record['Qualifier']}, "
                    f"Issuer: {record['Issuer']}, Valid Till: {record['Valid_Till']}"
                )
            kb_content.append("")
        return "\n".join(kb_content)
    except Exception as e:
        logger.error(f"Error creating knowledge base: {e}")
        return None


def create_enhanced_knowledge_base_from_rds() -> Tuple[Optional[str], int, int, Optional[str]]:
    """
    Create enhanced knowledge base text from RDS (same structure as /admin/refresh-knowledge-base).
    Returns: (kb_content, record_count, issuer_count, error)
    """
    try:
        logger.info("📚 Building enhanced knowledge base from RDS...")
        record_count = db.session.query(EmployeeRecord).count()
        if record_count == 0:
            return None, 0, 0, "No data in database"

        kb_lines: List[str] = []
        kb_lines.append("=" * 80)
        kb_lines.append("AI ACADEMY TRACKER - COMPREHENSIVE EMPLOYEE LEARNING DATABASE")
        kb_lines.append("=" * 80)
        kb_lines.append(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        kb_lines.append(f"Total Records: {record_count}")
        kb_lines.append("")

        total_employees = db.session.query(
            func.count(func.distinct(EmployeeRecord.employee_id))
        ).scalar()
        kb_lines.append("OVERALL STATISTICS")
        kb_lines.append("-" * 80)
        kb_lines.append(f"Total Unique Employees: {total_employees}")
        kb_lines.append("")
        kb_lines.append("OVERALL LEVEL DISTRIBUTION:")
        for level, count in db.session.query(
            EmployeeRecord.level,
            func.count(EmployeeRecord.id)
        ).group_by(
            EmployeeRecord.level
        ).order_by(
            EmployeeRecord.level
        ).all():
            kb_lines.append(f"  {level}: {count} records")
        kb_lines.append("")
        kb_lines.append("OVERALL QUALIFIER DISTRIBUTION:")
        for qual, count in db.session.query(
            EmployeeRecord.qualifier,
            func.count(EmployeeRecord.id)
        ).group_by(
            EmployeeRecord.qualifier
        ).all():
            kb_lines.append(f"  {qual}: {count} records")
        kb_lines.append("")
        kb_lines.append("TOP SKILLS:")
        top_skills = db.session.query(
            EmployeeRecord.skill,
            func.count(EmployeeRecord.id)
        ).filter(
            EmployeeRecord.skill.isnot(None),
            EmployeeRecord.skill != ''
        ).group_by(
            EmployeeRecord.skill
        ).order_by(
            func.count(EmployeeRecord.id).desc()
        ).limit(15).all()
        for skill, count in top_skills:
            kb_lines.append(f"  {skill}: {count} records")
        kb_lines.append("")
        kb_lines.append("TOP CERTIFICATIONS (ASSESSMENTS):")
        top_assessments = db.session.query(
            EmployeeRecord.assessment_name,
            func.count(EmployeeRecord.id)
        ).filter(
            EmployeeRecord.assessment_name.isnot(None),
            EmployeeRecord.assessment_name != ''
        ).group_by(
            EmployeeRecord.assessment_name
        ).order_by(
            func.count(EmployeeRecord.id).desc()
        ).limit(15).all()
        for assessment, count in top_assessments:
            kb_lines.append(f"  {assessment}: {count} records")
        kb_lines.append("")
        kb_lines.append("=" * 80)
        kb_lines.append("DETAILED BREAKDOWN BY ISSUER")
        kb_lines.append("=" * 80)
        kb_lines.append("")

        issuers = db.session.query(
            EmployeeRecord.issuer
        ).distinct().order_by(
            EmployeeRecord.issuer
        ).all()

        for (issuer,) in issuers:
            kb_lines.append(f"ISSUER: {issuer}")
            kb_lines.append("-" * 80)
            issuer_total = db.session.query(
                func.count(EmployeeRecord.id)
            ).filter(
                EmployeeRecord.issuer == issuer
            ).scalar()
            kb_lines.append(f"Total Records: {issuer_total}")
            kb_lines.append("")
            kb_lines.append(f"Level Distribution for {issuer}:")
            issuer_levels = db.session.query(
                EmployeeRecord.level,
                func.count(EmployeeRecord.id)
            ).filter(
                EmployeeRecord.issuer == issuer
            ).group_by(
                EmployeeRecord.level
            ).order_by(
                EmployeeRecord.level
            ).all()
            for level, count in issuer_levels:
                kb_lines.append(f"  {level}: {count} records")
            kb_lines.append("")
            kb_lines.append(f"Certification Status by Level for {issuer}:")
            issuer_level_qual = db.session.query(
                EmployeeRecord.level,
                EmployeeRecord.qualifier,
                func.count(EmployeeRecord.id)
            ).filter(
                EmployeeRecord.issuer == issuer
            ).group_by(
                EmployeeRecord.level,
                EmployeeRecord.qualifier
            ).order_by(
                EmployeeRecord.level,
                EmployeeRecord.qualifier
            ).all()
            for level, qualifier, count in issuer_level_qual:
                kb_lines.append(f"  {level} - {qualifier}: {count} employees")
            kb_lines.append("")
            kb_lines.append("")

        kb_lines.append("=" * 80)
        kb_lines.append("DISTRIBUTION BY WIPRO FUNCTION")
        kb_lines.append("=" * 80)
        function_dist = db.session.query(
            EmployeeRecord.wipro_function,
            func.count(EmployeeRecord.id)
        ).group_by(
            EmployeeRecord.wipro_function
        ).order_by(
            func.count(EmployeeRecord.id).desc()
        ).all()
        for function, count in function_dist:
            kb_lines.append(f"{function}: {count} records")
        kb_lines.append("")
        kb_lines.append("=" * 80)
        kb_lines.append("QUICK REFERENCE MATRIX")
        kb_lines.append("=" * 80)
        kb_lines.append("")
        for (issuer,) in issuers:
            kb_lines.append(f"{issuer}:")
            issuer_matrix = db.session.query(
                EmployeeRecord.level,
                EmployeeRecord.qualifier,
                func.count(EmployeeRecord.id)
            ).filter(
                EmployeeRecord.issuer == issuer
            ).group_by(
                EmployeeRecord.level,
                EmployeeRecord.qualifier
            ).order_by(
                EmployeeRecord.level,
                EmployeeRecord.qualifier
            ).all()
            for level, qualifier, count in issuer_matrix:
                kb_lines.append(f"  {level} {qualifier}: {count}")
            kb_lines.append("")
        kb_lines.append("=" * 80)
        kb_lines.append("EMPLOYEE LOOKUP CAPABILITY")
        kb_lines.append("=" * 80)
        kb_lines.append(
            "For specific employee queries, the system queries the live database"
        )
        kb_lines.append(
            "to retrieve current employee records with all certifications."
        )
        kb_lines.append("")

        kb_content = "\n".join(kb_lines)
        issuer_count = len(issuers)
        logger.info(f"Knowledge base created: {len(kb_content)} bytes, {record_count} records, {issuer_count} issuers")
        return kb_content, record_count, issuer_count, None
    except Exception as e:
        logger.error(f"Error creating enhanced knowledge base from RDS: {e}")
        return None, 0, 0, str(e)


def allowed_file(filename: str) -> bool:
    """Check if uploaded file is a CSV or XLSX."""
    try:
        if not filename:
            return False
        return filename.lower().endswith(('.csv', '.xlsx'))
    except Exception:
        return False
