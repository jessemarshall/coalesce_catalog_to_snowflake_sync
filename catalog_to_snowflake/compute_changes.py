#!/usr/bin/env python3
"""
Module for computing tag changes and generating unified change SQL

Computes differences between current and previous runs, categorizing changes as new, modified, or removed.
Generates a unified changes file with DROP statements first, then SET statements for safe execution order.
"""

import json
import logging
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime
from pathlib import Path

from catalog_to_snowflake.generate_sql import escape_sql_value, format_snowflake_identifier, parse_tag_label

logger = logging.getLogger(__name__)


def format_timestamp_for_comment(dt: Optional[datetime]) -> str:
    """
    Format a datetime object to human-readable string for SQL comments

    Args:
        dt: datetime object or None

    Returns:
        Formatted string like "2026-02-08 15:16:34" or empty string
    """
    if not dt:
        return ""

    try:
        # Format as readable date/time
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, AttributeError):
        # If formatting fails, return ISO format as fallback
        return dt.isoformat() if dt else ""


def load_previous_run_data(data_dir: str = "data") -> Tuple[Dict, str, Optional[datetime]]:
    """
    Load the most recent previous run's catalog columns data

    Args:
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (catalog_columns dict, filename, last_run_timestamp)
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        logger.warning(f"Data directory {data_dir} does not exist")
        return {}, "", None

    # Find all catalog files (try new naming first, then fall back to old)
    tables_columns_files = list(data_path.glob("catalog_tables_columns_*.json"))

    # Fall back to old naming for backwards compatibility
    if not tables_columns_files:
        tables_columns_files = list(data_path.glob("catalog_columns_*.json"))

    if not tables_columns_files:
        logger.info("No previous catalog files found")
        return {}, "", None

    # Sort by modification time and get the most recent
    tables_columns_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    latest_file = tables_columns_files[0]

    logger.info(f"Loading previous run data from: {latest_file}")

    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)

        # Try to extract the run timestamp from the filename or file metadata
        last_run_timestamp = None
        filename = latest_file.name

        # Try to parse timestamp from filename (format: catalog_tables_columns_YYYYMMDD_HHMMSS.json or catalog_columns_YYYYMMDD_HHMMSS.json)
        try:
            timestamp_str = filename.replace("catalog_tables_columns_", "").replace("catalog_columns_", "").replace(".json", "")
            last_run_timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            logger.info(f"Previous run timestamp: {last_run_timestamp}")
        except (ValueError, IndexError):
            # Fall back to file modification time
            last_run_timestamp = datetime.fromtimestamp(latest_file.stat().st_mtime)
            logger.info(f"Using file modification time as run timestamp: {last_run_timestamp}")

        return data, filename, last_run_timestamp
    except Exception as e:
        logger.error(f"Failed to load previous run data: {e}")
        return {}, "", None


def parse_timestamp(timestamp_value) -> Optional[datetime]:
    """
    Parse a timestamp (string or int) into a datetime object

    Args:
        timestamp_value: Timestamp from the API (could be string or int)

    Returns:
        datetime object or None if parsing fails
    """
    if not timestamp_value:
        return None

    try:
        # Handle integer timestamps (milliseconds)
        if isinstance(timestamp_value, (int, float)):
            # Assume milliseconds if value is large
            if timestamp_value > 10000000000:  # Greater than year 2001 in seconds
                return datetime.fromtimestamp(timestamp_value / 1000)
            else:
                return datetime.fromtimestamp(timestamp_value)

        # Handle string timestamps
        timestamp_str = str(timestamp_value)

        # ISO format with Z: 2024-01-15T10:30:00Z
        if 'T' in timestamp_str:
            if timestamp_str.endswith('Z'):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(timestamp_str)
        # Unix timestamp (numeric string)
        elif timestamp_str.isdigit():
            ts = int(timestamp_str)
            # Assume milliseconds if value is large
            if ts > 10000000000:  # Greater than year 2001 in seconds
                return datetime.fromtimestamp(ts / 1000)
            else:
                return datetime.fromtimestamp(ts)
        else:
            return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        logger.debug(f"Could not parse timestamp '{timestamp_value}': {e}")
        return None


def extract_table_column_tags_with_timestamps(catalog_columns: Dict) -> Tuple[
    Dict[str, Tuple[Dict[str, str], Optional[datetime]]],
    Dict[Tuple[str, str], Tuple[Dict[str, str], Optional[datetime]]]
]:
    """
    Extract all table and column tags with their update timestamps from catalog data

    Args:
        catalog_columns: Dictionary of catalog columns data

    Returns:
        Tuple of (table_tags_with_timestamps, column_tags_with_timestamps)
        - table_tags_with_timestamps: {full_table_name: ({tag_key: tag_value}, updatedAt)}
        - column_tags_with_timestamps: {(full_table_name, column_name): ({tag_key: tag_value}, updatedAt)}
    """
    table_tags = {}
    column_tags = {}

    for table_id, table_data in catalog_columns.items():
        table_info = table_data.get("table", {})
        schema = table_info.get("schema", {})
        database = schema.get("database", {})

        # Skip if table metadata is missing (old format data)
        if not database.get('name') or not schema.get('name') or not table_info.get('name'):
            logger.debug(f"Skipping table {table_id} - missing metadata")
            continue

        full_table_name = f"{database.get('name')}.{schema.get('name')}.{table_info.get('name')}"

        # Extract table-level tags
        table_tag_entities = table_info.get("tagEntities", [])
        if table_tag_entities:
            tags_dict = {}
            latest_timestamp = None
            for tag_entity in table_tag_entities:
                tag_label = tag_entity.get("tag", {}).get("label", "")
                # Get the TagEntity's own updatedAt timestamp
                tag_updated = parse_timestamp(tag_entity.get("updatedAt", ""))
                if tag_label:
                    key, value = parse_tag_label(tag_label)
                    tags_dict[key.upper()] = value
                    # Keep track of the most recent tag update
                    if tag_updated and (not latest_timestamp or tag_updated > latest_timestamp):
                        latest_timestamp = tag_updated
            if tags_dict:
                table_tags[full_table_name] = (tags_dict, latest_timestamp)

        # Extract column-level tags (only if we have valid table metadata)
        columns = table_data.get("columns", [])
        for column in columns:
            column_name = column.get("name", "")
            column_tag_entities = column.get("tagEntities", [])

            if column_tag_entities:
                tags_dict = {}
                latest_timestamp = None
                for tag_entity in column_tag_entities:
                    tag_label = tag_entity.get("tag", {}).get("label", "")
                    # Get the TagEntity's own updatedAt timestamp
                    tag_updated = parse_timestamp(tag_entity.get("updatedAt", ""))
                    if tag_label:
                        key, value = parse_tag_label(tag_label)
                        tags_dict[key.upper()] = value
                        # Keep track of the most recent tag update
                        if tag_updated and (not latest_timestamp or tag_updated > latest_timestamp):
                            latest_timestamp = tag_updated
                if tags_dict:
                    column_tags[(full_table_name, column_name)] = (tags_dict, latest_timestamp)

    return table_tags, column_tags


def categorize_tag_changes(
    previous_data: Dict,
    current_data: Dict,
    last_run_timestamp: Optional[datetime] = None
) -> Dict:
    """
    Categorize tag changes into new, modified, and removed

    Args:
        previous_data: Previous run's catalog columns data
        current_data: Current run's catalog columns data
        last_run_timestamp: Timestamp of the last run

    Returns:
        Dictionary with categorized changes
    """
    # Extract tags with timestamps from both runs
    prev_table_tags, prev_column_tags = extract_table_column_tags_with_timestamps(previous_data)
    curr_table_tags, curr_column_tags = extract_table_column_tags_with_timestamps(current_data)

    # Initialize result structure
    changes = {
        'new': {
            'tables': {},  # Completely new tables with tags
            'columns': {},  # Completely new columns with tags
        },
        'modified': {
            'tables': {},  # Existing tables with updated tags
            'columns': {},  # Existing columns with updated tags
        },
        'removed': {
            'tables': {},  # Tags removed from tables
            'columns': {},  # Tags removed from columns
        }
    }

    # Process tables
    for table, (curr_tags, curr_updated) in curr_table_tags.items():
        if table not in prev_table_tags:
            # Completely new table
            changes['new']['tables'][table] = (curr_tags, curr_updated)
            logger.info(f"New table found: {table} with tags: {curr_tags}")
        else:
            prev_tags, prev_updated = prev_table_tags[table]
            # Convert dicts to sets of keys for comparison
            curr_keys = set(curr_tags.keys())
            prev_keys = set(prev_tags.keys())
            added_keys = curr_keys - prev_keys
            removed_keys = prev_keys - curr_keys

            # Check if table was modified since last run
            if last_run_timestamp and curr_updated and curr_updated > last_run_timestamp:
                if added_keys or removed_keys:
                    added_tags = {k: curr_tags[k] for k in added_keys}
                    removed_tags = {k: prev_tags[k] for k in removed_keys}
                    changes['modified']['tables'][table] = {
                        'added': added_tags,
                        'removed': removed_tags,
                        'current': curr_tags,
                        'updated_at': curr_updated
                    }
                    logger.info(f"Table {table} modified since last run - Added tags: {added_keys}, Removed tags: {removed_keys}")

            # Track removed tags separately for DROP statements
            if removed_keys:
                changes['removed']['tables'][table] = {k: prev_tags[k] for k in removed_keys}

    # Find tables that no longer exist
    for table, (prev_tags, _) in prev_table_tags.items():
        if table not in curr_table_tags:
            changes['removed']['tables'][table] = prev_tags
            logger.info(f"Table {table} no longer exists, removing all tags: {prev_tags}")

    # Process columns
    for (table, column), (curr_tags, curr_updated) in curr_column_tags.items():
        if (table, column) not in prev_column_tags:
            # Completely new column
            changes['new']['columns'][(table, column)] = (curr_tags, curr_updated)
            logger.info(f"New column found: {table}.{column} with tags: {curr_tags}")
        else:
            prev_tags, prev_updated = prev_column_tags[(table, column)]
            # Convert dicts to sets of keys for comparison
            curr_keys = set(curr_tags.keys())
            prev_keys = set(prev_tags.keys())
            added_keys = curr_keys - prev_keys
            removed_keys = prev_keys - curr_keys

            # Check if column was modified since last run
            if last_run_timestamp and curr_updated and curr_updated > last_run_timestamp:
                if added_keys or removed_keys:
                    added_tags = {k: curr_tags[k] for k in added_keys}
                    removed_tags = {k: prev_tags[k] for k in removed_keys}
                    changes['modified']['columns'][(table, column)] = {
                        'added': added_tags,
                        'removed': removed_tags,
                        'current': curr_tags,
                        'updated_at': curr_updated
                    }
                    logger.info(f"Column {table}.{column} modified since last run - Added tags: {added_keys}, Removed tags: {removed_keys}")

            # Track removed tags separately for DROP statements
            if removed_keys:
                changes['removed']['columns'][(table, column)] = {k: prev_tags[k] for k in removed_keys}

    # Find columns that no longer exist
    for (table, column), (prev_tags, _) in prev_column_tags.items():
        if (table, column) not in curr_column_tags:
            changes['removed']['columns'][(table, column)] = prev_tags
            logger.info(f"Column {table}.{column} no longer exists, removing all tags: {prev_tags}")

    return changes


def generate_new_tags_sql(changes: Dict, previous_filename: str, last_run_timestamp: Optional[datetime] = None) -> str:
    """
    Generate SQL for completely new tags (new tables/columns)

    Args:
        changes: Categorized changes dictionary
        previous_filename: Name of the previous run file
        last_run_timestamp: Timestamp of the last run

    Returns:
        SQL file content as string
    """
    statements = []
    new_tables = changes['new']['tables']
    new_columns = changes['new']['columns']

    # Build header
    header = [
        "-- ============================================================",
        "-- NEW TAGS SQL Script",
        "-- Tags for completely new tables and columns",
        f"-- Generated: {datetime.now().isoformat()}",
        f"-- Compared with: {previous_filename}",
    ]

    if last_run_timestamp:
        header.append(f"-- Previous run: {last_run_timestamp.isoformat()}")

    header.extend([
        "-- ============================================================",
        "",
        "-- This script contains tags for:",
        "-- 1. Tables that didn't exist in the previous run",
        "-- 2. Columns that didn't exist in the previous run",
        "",
        "-- ============================================================",
        ""
    ])

    # Generate statements for new tables
    if new_tables:
        statements.append("-- ============================================================")
        statements.append("-- NEW TABLE TAGS")
        statements.append(f"-- Tables: {len(new_tables)}")
        statements.append("-- ============================================================")
        statements.append("")

        for table, (tags, updated_at) in sorted(new_tables.items()):
            statements.append(f"-- Table: {table}")
            if updated_at:
                statements.append(f"-- Created/Updated: {updated_at.isoformat()}")
            statements.append(f"-- Tags to add: {', '.join(sorted(tags.keys()))}")
            for tag_key, tag_value in sorted(tags.items()):
                tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                readable_timestamp = format_timestamp_for_comment(updated_at)
                timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                statements.append(f"ALTER TABLE {table}")
                statements.append(f"    SET TAG {tag_name} = '{escape_sql_value(tag_value)}';{timestamp_comment}")
            statements.append("")

    # Generate statements for new columns
    if new_columns:
        statements.append("-- ============================================================")
        statements.append("-- NEW COLUMN TAGS")
        statements.append(f"-- Columns: {len(new_columns)}")
        statements.append("-- ============================================================")
        statements.append("")

        # Group by table for better organization
        columns_by_table = {}
        for (table, column), (tags, updated_at) in new_columns.items():
            if table not in columns_by_table:
                columns_by_table[table] = []
            columns_by_table[table].append((column, tags, updated_at))

        for table in sorted(columns_by_table.keys()):
            statements.append(f"-- Table: {table}")
            for column, tags, updated_at in sorted(columns_by_table[table]):
                statements.append(f"-- Column: {column}")
                if updated_at:
                    statements.append(f"-- Created/Updated: {updated_at.isoformat()}")
                statements.append(f"-- Tags to add: {', '.join(sorted(tags.keys()))}")
                for tag_key, tag_value in sorted(tags.items()):
                    tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                    readable_timestamp = format_timestamp_for_comment(updated_at)
                    timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                    statements.append(f"ALTER TABLE {table}")
                    statements.append(f"    ALTER COLUMN {column}")
                    statements.append(f"        SET TAG {tag_name} = '{escape_sql_value(tag_value)}';{timestamp_comment}")
                statements.append("")

    # Build footer
    total_count = len(new_tables) + len(new_columns)
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   New tables with tags: {len(new_tables)}",
        f"--   New columns with tags: {len(new_columns)}",
        f"--   Total new entities: {total_count}",
        "-- ============================================================",
        "-- End of NEW TAGS script"
    ]

    if statements:
        return "\n".join(header + statements + footer)
    else:
        no_new = [
            "-- No new tables or columns with tags found",
            "-- All entities existed in the previous run"
        ]
        return "\n".join(header + no_new + footer)


def generate_modified_tags_sql(changes: Dict, previous_filename: str, last_run_timestamp: Optional[datetime] = None) -> str:
    """
    Generate SQL for modified tags (existing tables/columns that were updated)

    Args:
        changes: Categorized changes dictionary
        previous_filename: Name of the previous run file
        last_run_timestamp: Timestamp of the last run

    Returns:
        SQL file content as string
    """
    statements = []
    modified_tables = changes['modified']['tables']
    modified_columns = changes['modified']['columns']

    # Build header
    header = [
        "-- ============================================================",
        "-- MODIFIED TAGS SQL Script",
        "-- Tag changes for existing tables and columns updated since last run",
        f"-- Generated: {datetime.now().isoformat()}",
        f"-- Compared with: {previous_filename}",
    ]

    if last_run_timestamp:
        header.append(f"-- Previous run: {last_run_timestamp.isoformat()}")

    header.extend([
        "-- ============================================================",
        "",
        "-- This script contains tag modifications for:",
        "-- 1. Tables that existed before but were updated since last run",
        "-- 2. Columns that existed before but were updated since last run",
        "",
        "-- NOTE: Review these changes carefully as they show tag evolution",
        "",
        "-- ============================================================",
        ""
    ])

    # Generate statements for modified tables
    if modified_tables:
        statements.append("-- ============================================================")
        statements.append("-- MODIFIED TABLE TAGS")
        statements.append(f"-- Tables modified: {len(modified_tables)}")
        statements.append("-- ============================================================")
        statements.append("")

        for table, info in sorted(modified_tables.items()):
            statements.append(f"-- Table: {table}")
            if info.get('updated_at'):
                statements.append(f"-- Last updated: {info['updated_at'].isoformat()}")

            if info['removed']:
                statements.append(f"-- Tags removed: {', '.join(sorted(info['removed'].keys()))}")
            if info['added']:
                statements.append(f"-- Tags added: {', '.join(sorted(info['added'].keys()))}")
            statements.append(f"-- Current tags: {', '.join(sorted(info['current'].keys()))}")

            # Generate UNSET statements for removed tags
            for tag_key in sorted(info['removed'].keys()):
                tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                readable_timestamp = format_timestamp_for_comment(info.get('updated_at'))
                timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                statements.append(f"ALTER TABLE {table}")
                statements.append(f"    UNSET TAG {tag_name};{timestamp_comment}")

            # Generate SET statements for added tags
            for tag_key, tag_value in sorted(info['added'].items()):
                tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                readable_timestamp = format_timestamp_for_comment(info.get('updated_at'))
                timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                statements.append(f"ALTER TABLE {table}")
                statements.append(f"    SET TAG {tag_name} = '{escape_sql_value(tag_value)}';{timestamp_comment}")

            statements.append("")

    # Generate statements for modified columns
    if modified_columns:
        statements.append("-- ============================================================")
        statements.append("-- MODIFIED COLUMN TAGS")
        statements.append(f"-- Columns modified: {len(modified_columns)}")
        statements.append("-- ============================================================")
        statements.append("")

        # Group by table for better organization
        columns_by_table = {}
        for (table, column), info in modified_columns.items():
            if table not in columns_by_table:
                columns_by_table[table] = []
            columns_by_table[table].append((column, info))

        for table in sorted(columns_by_table.keys()):
            statements.append(f"-- Table: {table}")
            for column, info in sorted(columns_by_table[table]):
                statements.append(f"-- Column: {column}")
                if info.get('updated_at'):
                    statements.append(f"-- Last updated: {info['updated_at'].isoformat()}")

                if info['removed']:
                    statements.append(f"-- Tags removed: {', '.join(sorted(info['removed'].keys()))}")
                if info['added']:
                    statements.append(f"-- Tags added: {', '.join(sorted(info['added'].keys()))}")
                statements.append(f"-- Current tags: {', '.join(sorted(info['current'].keys()))}")

                # Generate UNSET statements for removed tags
                for tag_key in sorted(info['removed'].keys()):
                    tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                    readable_timestamp = format_timestamp_for_comment(info.get('updated_at'))
                    timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                    statements.append(f"ALTER TABLE {table}")
                    statements.append(f"    ALTER COLUMN {column}")
                    statements.append(f"        UNSET TAG {tag_name};{timestamp_comment}")

                # Generate SET statements for added tags
                for tag_key, tag_value in sorted(info['added'].items()):
                    tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                    readable_timestamp = format_timestamp_for_comment(info.get('updated_at'))
                    timestamp_comment = f"  -- updatedAt: {readable_timestamp}" if readable_timestamp else ""
                    statements.append(f"ALTER TABLE {table}")
                    statements.append(f"    ALTER COLUMN {column}")
                    statements.append(f"        SET TAG {tag_name} = '{escape_sql_value(tag_value)}';{timestamp_comment}")

                statements.append("")

    # Build footer
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   Modified tables: {len(modified_tables)}",
        f"--   Modified columns: {len(modified_columns)}",
        f"--   Total modified entities: {len(modified_tables) + len(modified_columns)}",
        "-- ============================================================",
        "-- End of MODIFIED TAGS script"
    ]

    if statements:
        return "\n".join(header + statements + footer)
    else:
        no_modified = [
            "-- No modified tags found",
            "-- No entities were updated since the last run"
        ]
        return "\n".join(header + no_modified + footer)


def generate_drop_tags_sql(changes: Dict, previous_filename: str, last_run_timestamp: Optional[datetime] = None) -> str:
    """
    Generate SQL for dropping removed tags

    Args:
        changes: Categorized changes dictionary
        previous_filename: Name of the previous run file
        last_run_timestamp: Timestamp of the last run

    Returns:
        SQL file content as string
    """
    statements = []
    removed_table_tags = changes['removed']['tables']
    removed_column_tags = changes['removed']['columns']

    # Build header
    header = [
        "-- ============================================================",
        "-- DROP TAGS SQL Script",
        "-- Remove tags that are no longer present in the catalog",
        f"-- Generated: {datetime.now().isoformat()}",
        f"-- Compared with: {previous_filename}",
    ]

    if last_run_timestamp:
        header.append(f"-- Previous run: {last_run_timestamp.isoformat()}")

    header.extend([
        "-- ============================================================",
        "",
        "-- This script will:",
        "-- 1. Remove tags from tables where they no longer exist",
        "-- 2. Remove tags from columns where they no longer exist",
        "-- 3. Remove all tags from deleted tables/columns",
        "",
        "-- IMPORTANT: Review these changes carefully before executing",
        "-- These tags will be permanently removed from Snowflake objects",
        "",
        "-- ============================================================",
        ""
    ])

    # Generate DROP statements for table tags
    if removed_table_tags:
        statements.append("-- ============================================================")
        statements.append("-- TABLE TAGS TO REMOVE")
        statements.append(f"-- Tables affected: {len(removed_table_tags)}")
        statements.append("-- ============================================================")
        statements.append("")

        for table, tags in sorted(removed_table_tags.items()):
            statements.append(f"-- Table: {table}")
            # Handle both dict and set formats for backward compatibility
            if isinstance(tags, dict):
                tag_keys = tags.keys()
            else:
                tag_keys = tags
            statements.append(f"-- Tags to remove: {', '.join(sorted(tag_keys))}")
            for tag_key in sorted(tag_keys):
                tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                statements.append(f"ALTER TABLE {table}")
                statements.append(f"    UNSET TAG {tag_name};")
            statements.append("")

    # Generate DROP statements for column tags
    if removed_column_tags:
        statements.append("-- ============================================================")
        statements.append("-- COLUMN TAGS TO REMOVE")
        statements.append(f"-- Columns affected: {len(removed_column_tags)}")
        statements.append("-- ============================================================")
        statements.append("")

        # Group by table for better organization
        columns_by_table = {}
        for (table, column), tags in removed_column_tags.items():
            if table not in columns_by_table:
                columns_by_table[table] = []
            columns_by_table[table].append((column, tags))

        for table in sorted(columns_by_table.keys()):
            statements.append(f"-- Table: {table}")
            for column, tags in sorted(columns_by_table[table]):
                statements.append(f"-- Column: {column}")
                # Handle both dict and set formats for backward compatibility
                if isinstance(tags, dict):
                    tag_keys = tags.keys()
                else:
                    tag_keys = tags
                statements.append(f"-- Tags to remove: {', '.join(sorted(tag_keys))}")
                for tag_key in sorted(tag_keys):
                    tag_name = tag_key.replace(" ", "_").replace("-", "_").upper()
                    statements.append(f"ALTER TABLE {table}")
                    statements.append(f"    ALTER COLUMN {column}")
                    statements.append(f"        UNSET TAG {tag_name};")
                statements.append("")

    # Count actual DROP statements
    drop_count = len([s for s in statements if "UNSET TAG" in s])

    # Build footer
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   Tables with tags to remove: {len(removed_table_tags)}",
        f"--   Columns with tags to remove: {len(removed_column_tags)}",
        f"--   Total UNSET TAG statements: {drop_count}",
        "-- ============================================================",
        "-- End of DROP TAGS script"
    ]

    if statements:
        return "\n".join(header + statements + footer)
    else:
        no_drops = [
            "-- No tags to drop",
            "-- All tags from the previous run are still present"
        ]
        return "\n".join(header + no_drops + footer)


def process_tag_changes(current_catalog_columns: Dict, data_dir: str = "data") -> Tuple[str, str, str, Dict]:
    """
    Process tag changes and generate SQL for new, modified, and dropped tags

    Args:
        current_catalog_columns: Current run's catalog columns data
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (new_tags_sql, modified_tags_sql, drop_tags_sql, statistics)
        Note: These are used internally to build the unified change file
    """
    # Load previous run data
    previous_file_data, previous_filename, last_run_timestamp = load_previous_run_data(data_dir)

    if not previous_file_data:
        logger.info("No previous run data found - treating all tags as new")
        # If no previous data, all current tags are new
        changes = {
            'new': {'tables': {}, 'columns': {}},
            'modified': {'tables': {}, 'columns': {}},
            'removed': {'tables': {}, 'columns': {}}
        }

        # Extract current tags as all new
        curr_table_tags, curr_column_tags = extract_table_column_tags_with_timestamps(current_catalog_columns)
        changes['new']['tables'] = curr_table_tags
        changes['new']['columns'] = curr_column_tags

        new_sql = generate_new_tags_sql(changes, "N/A (first run)", None)
        modified_sql = generate_modified_tags_sql(changes, "N/A (first run)", None)
        drop_sql = generate_drop_tags_sql(changes, "N/A (first run)", None)

        stats = {
            'new_tables': len(changes['new']['tables']),
            'new_columns': len(changes['new']['columns']),
            'modified_tables': 0,
            'modified_columns': 0,
            'removed_table_tags': 0,
            'removed_column_tags': 0
        }

        return new_sql, modified_sql, drop_sql, stats

    # Extract the catalog data from the saved structure (try new format first, then old)
    if "catalog_tables_columns" in previous_file_data:
        previous_data = previous_file_data["catalog_tables_columns"]
    elif "catalog_columns" in previous_file_data:
        # Support old naming for backwards compatibility
        previous_data = previous_file_data["catalog_columns"]
    elif "columns_by_table" in previous_file_data:
        # Old format doesn't have table metadata
        logger.warning("Previous run data is in old format without table metadata")
        logger.warning("Cannot properly categorize changes - treating all as new")

        changes = {
            'new': {'tables': {}, 'columns': {}},
            'modified': {'tables': {}, 'columns': {}},
            'removed': {'tables': {}, 'columns': {}}
        }

        curr_table_tags, curr_column_tags = extract_table_column_tags_with_timestamps(current_catalog_columns)
        changes['new']['tables'] = curr_table_tags
        changes['new']['columns'] = curr_column_tags

        new_sql = generate_new_tags_sql(changes, previous_filename, last_run_timestamp)
        modified_sql = generate_modified_tags_sql(changes, previous_filename, last_run_timestamp)
        drop_sql = generate_drop_tags_sql(changes, previous_filename, last_run_timestamp)

        stats = {
            'new_tables': len(changes['new']['tables']),
            'new_columns': len(changes['new']['columns']),
            'modified_tables': 0,
            'modified_columns': 0,
            'removed_table_tags': 0,
            'removed_column_tags': 0
        }

        return new_sql, modified_sql, drop_sql, stats
    else:
        # Assume it's already in the right format
        previous_data = previous_file_data

    # Categorize all changes
    changes = categorize_tag_changes(previous_data, current_catalog_columns, last_run_timestamp)

    # Generate three SQL files
    new_sql = generate_new_tags_sql(changes, previous_filename, last_run_timestamp)
    modified_sql = generate_modified_tags_sql(changes, previous_filename, last_run_timestamp)
    drop_sql = generate_drop_tags_sql(changes, previous_filename, last_run_timestamp)

    # Calculate statistics
    stats = {
        'new_tables': len(changes['new']['tables']),
        'new_columns': len(changes['new']['columns']),
        'modified_tables': len(changes['modified']['tables']),
        'modified_columns': len(changes['modified']['columns']),
        'removed_table_tags': len(changes['removed']['tables']),
        'removed_column_tags': len(changes['removed']['columns'])
    }

    # Log summary
    logger.info("Tag change analysis complete:")
    logger.info(f"  • New tables with tags: {stats['new_tables']}")
    logger.info(f"  • New columns with tags: {stats['new_columns']}")
    logger.info(f"  • Modified tables: {stats['modified_tables']}")
    logger.info(f"  • Modified columns: {stats['modified_columns']}")
    logger.info(f"  • Tables with removed tags: {stats['removed_table_tags']}")
    logger.info(f"  • Columns with removed tags: {stats['removed_column_tags']}")

    return new_sql, modified_sql, drop_sql, stats


# Maintain backward compatibility
def process_drop_tags(current_catalog_columns: Dict, data_dir: str = "data") -> Tuple[str, int]:
    """
    Backward compatible function that returns only drop SQL

    Args:
        current_catalog_columns: Current run's catalog columns data
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (drop_sql_content, number_of_drops)
    """
    _, _, drop_sql, stats = process_tag_changes(current_catalog_columns, data_dir)

    # Count UNSET TAG statements in the SQL
    drop_count = len([line for line in drop_sql.split('\n') if 'UNSET TAG' in line])

    return drop_sql, drop_count


def generate_unified_change_sql(current_catalog_columns: Dict, data_dir: str = "data") -> Tuple[str, Dict]:
    """
    Generate a single unified SQL file with DROP statements first, then all SET statements

    Args:
        current_catalog_columns: Current run's catalog columns data
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (unified_sql_content, statistics_dict)
        Returns ("", empty stats) if no previous run exists (first run)
    """
    # First check if there's a previous run
    previous_file_data, previous_filename, last_run_timestamp = load_previous_run_data(data_dir)

    if not previous_file_data:
        # No previous run - don't generate a changes file
        logger.info("No previous run data found - skipping changes file generation")
        return "", {}

    # Get the changes using existing logic
    new_sql, modified_sql, drop_sql, stats = process_tag_changes(current_catalog_columns, data_dir)

    statements = []

    # Header
    statements.append("-- ============================================================")
    statements.append("-- UNIFIED TAG CHANGES SQL Script")
    statements.append("-- Organized with DROP statements first, then SET statements")
    statements.append("-- ============================================================")
    statements.append(f"-- Generated: {datetime.now().isoformat()}")
    statements.append("")

    # Add summary statistics
    statements.append("-- Summary:")
    statements.append(f"--   New tables with tags: {stats['new_tables']}")
    statements.append(f"--   New columns with tags: {stats['new_columns']}")
    statements.append(f"--   Modified tables: {stats['modified_tables']}")
    statements.append(f"--   Modified columns: {stats['modified_columns']}")
    statements.append(f"--   Tables with removed tags: {stats['removed_table_tags']}")
    statements.append(f"--   Columns with removed tags: {stats['removed_column_tags']}")
    statements.append("")

    # Section 1: DROP statements (cleanup first)
    drop_lines = drop_sql.split('\n') if drop_sql else []
    drop_statements = [line for line in drop_lines if 'UNSET TAG' in line or (line.startswith('--') and 'DROP' not in line and 'End of' not in line)]

    if any('UNSET TAG' in line for line in drop_statements):
        statements.append("-- ============================================================")
        statements.append("-- SECTION 1: DROP/UNSET TAGS (Cleanup)")
        statements.append("-- Review carefully before executing")
        statements.append("-- ============================================================")
        statements.append("")

        current_table = None
        for line in drop_lines:
            # Skip header/footer lines
            if any(skip in line for skip in ['============', 'DROP TAGS SQL Script', 'Generated:', 'Compared with:',
                                            'Previous run:', 'This script', 'Summary:', 'Total', 'End of']):
                continue
            # Include table/column headers and UNSET statements
            if line.strip() and ('Table:' in line or 'Column:' in line or 'UNSET TAG' in line or 'Tags to remove:' in line or 'removed from catalog' in line):
                if '-- Table:' in line:
                    if current_table:
                        statements.append("")  # Add spacing between tables
                    current_table = line
                statements.append(line)
        statements.append("")

    # Section 2: NEW and MODIFIED tags (all SET statements)
    statements.append("-- ============================================================")
    statements.append("-- SECTION 2: NEW AND MODIFIED TAGS (Apply Updates)")
    statements.append("-- ============================================================")
    statements.append("")

    # Process NEW tags
    new_lines = new_sql.split('\n') if new_sql else []
    has_new = any('SET TAG' in line for line in new_lines)

    if has_new:
        statements.append("-- ---------- NEW TAGS ----------")
        current_table = None
        for line in new_lines:
            # Skip header/footer lines
            if any(skip in line for skip in ['============', 'NEW TAGS SQL Script', 'Generated:', 'Compared with:',
                                            'Previous run:', 'This script', 'Summary:', 'Total', 'End of', '-- No new']):
                continue
            # Include relevant content
            if line.strip() and not line.startswith('-- ------'):
                if '-- Table:' in line:
                    if current_table:
                        statements.append("")  # Add spacing between tables
                    current_table = line
                statements.append(line)
        if current_table:  # Add final spacing if we had content
            statements.append("")

    # Process MODIFIED tags
    modified_lines = modified_sql.split('\n') if modified_sql else []
    has_modified = any('SET TAG' in line for line in modified_lines)

    if has_modified:
        statements.append("-- ---------- MODIFIED TAGS ----------")
        current_table = None
        for line in modified_lines:
            # Skip header/footer lines and UNSET lines (already in DROP section)
            if any(skip in line for skip in ['============', 'MODIFIED TAGS SQL Script', 'Generated:', 'Compared with:',
                                            'Previous run:', 'This script', 'Summary:', 'Total', 'End of', 'UNSET TAG', '-- No changes']):
                continue
            # Include relevant content (SET statements only)
            if line.strip() and not line.startswith('-- ------'):
                if '-- Table:' in line:
                    if current_table:
                        statements.append("")  # Add spacing between tables
                    current_table = line
                    statements.append(line)
                elif 'Tags removed:' not in line:  # Skip the removed tags comment
                    statements.append(line)
        if current_table:  # Add final spacing if we had content
            statements.append("")

    # Footer
    statements.append("")
    statements.append("-- ============================================================")
    statements.append("-- End of unified tag changes script")
    statements.append("-- ============================================================")

    return "\n".join(statements), stats


def create_new_tags_sql(catalog_columns: Dict[str, Dict], previous_timestamp: str = None) -> str:
    """
    Generate SQL for all current tags as NEW tags (used with --force-all)

    Args:
        catalog_columns: Current catalog data
        previous_timestamp: Optional timestamp string for header

    Returns:
        SQL content string treating all tags as new
    """
    statements = []

    # Header
    header = [
        "-- ============================================================",
        "-- FORCED NEW TAGS SQL Script (--force-all)",
        "-- All current tags treated as NEW regardless of sync history",
        "-- ============================================================",
        f"-- Generated: {datetime.now().isoformat()}"
    ]

    if previous_timestamp:
        header.append(f"-- Previous run: {previous_timestamp}")

    header.extend([
        "",
        "-- NOTICE: This file contains ALL current tags as NEW tags",
        "-- Use this for initial setup or complete tag refresh",
        "",
        "-- ============================================================",
        ""
    ])

    table_count = 0
    column_count = 0

    # Process all tables as new
    for table_id, table_data in sorted(catalog_columns.items()):
        table = table_data.get("table", {})
        columns = table_data.get("columns", [])

        schema = table.get("schema", {})
        database = schema.get("database", {})
        full_table_path = f"{database.get('name')}.{schema.get('name')}.{table.get('name')}"

        table_has_tags = False
        table_statements = []

        # Table header
        table_statements.append(f"-- Table: {full_table_path}")

        # Process table-level tags
        table_tag_entities = table.get("tagEntities", [])
        if table_tag_entities:
            table_has_tags = True
            table_count += 1
            table_statements.append(f"-- Table tags: {len(table_tag_entities)}")

            for tag_entity in table_tag_entities:
                tag_label = tag_entity.get("tag", {}).get("label", "")
                updated_at = tag_entity.get("updatedAt", "")

                if tag_label:
                    key, value = parse_tag_label(tag_label)
                    tag_name = format_snowflake_identifier(key)

                    # Format timestamp from milliseconds
                    timestamp_comment = ""
                    if updated_at:
                        try:
                            dt = datetime.fromtimestamp(updated_at / 1000)
                            readable_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                            timestamp_comment = f"  -- Tag applied: {readable_timestamp}"
                        except (ValueError, TypeError, OSError):
                            pass

                    table_statements.append(f"ALTER TABLE {full_table_path}")
                    table_statements.append(f"    SET TAG {tag_name} = '{escape_sql_value(value)}';{timestamp_comment}")
            table_statements.append("")

        # Process column-level tags
        if columns:
            for column in columns:
                column_name = column.get("name", "")
                tag_entities = column.get("tagEntities", [])

                if tag_entities:
                    table_has_tags = True
                    column_count += 1
                    table_statements.append(f"-- Column: {column_name}")

                    for tag_entity in tag_entities:
                        tag_label = tag_entity.get("tag", {}).get("label", "")
                        updated_at = tag_entity.get("updatedAt", "")

                        if tag_label:
                            key, value = parse_tag_label(tag_label)
                            tag_name = format_snowflake_identifier(key)

                            # Format timestamp from milliseconds
                            timestamp_comment = ""
                            if updated_at:
                                try:
                                    dt = datetime.fromtimestamp(updated_at / 1000)
                                    readable_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                                    timestamp_comment = f"  -- Tag applied: {readable_timestamp}"
                                except (ValueError, TypeError, OSError):
                                    pass

                            table_statements.append(f"ALTER TABLE {full_table_path}")
                            table_statements.append(f"    ALTER COLUMN {column_name}")
                            table_statements.append(f"        SET TAG {tag_name} = '{escape_sql_value(value)}';{timestamp_comment}")
                    table_statements.append("")

        # Add table statements if it has any tags
        if table_has_tags:
            statements.extend(table_statements)

    # Footer
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   Tables with tags: {table_count}",
        f"--   Columns with tags: {column_count}",
        f"--   Total entities: {table_count + column_count}",
        "-- ============================================================",
        "-- End of forced NEW tags script"
    ]

    return "\n".join(header + statements + footer)