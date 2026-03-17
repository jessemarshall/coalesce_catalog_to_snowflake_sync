#!/usr/bin/env python3
"""
Main Orchestrator - Coalesce Catalog to Snowflake Sync

This script orchestrates the complete workflow by importing and using modular components:
1. Fetches Snowflake tables with tags
2. Gets catalog columns for those tables
3. Generates SQL ALTER statements to apply tags in Snowflake
4. Generates a unified changes file with DROP statements first, then SET statements

Change Tracking:
    Change tracking (changes_since_last_full_run_*.sql) is generated automatically on full runs.
    Limited runs (--table-id, --table-ids, or --limit) will only generate the complete_current_state_*.sql file.
    This prevents misleading comparisons against previous full runs.
    The changes file is only generated when there's previous data to compare against (not on first run).

Usage:
    python main.py [options]

Example:
    python main.py                                                    # Process ALL tables (default)
    python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109  # Process specific table only
    python main.py --limit 5                                          # Test with 5 tables
    python main.py --force-all                                        # Treat all tags as NEW (ignore history)
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Import our modules
from catalog_to_snowflake import (
    CatalogAPIClient,
    get_snowflake_warehouse_ids,
    get_all_snowflake_tables,
    fetch_table_by_id,
    process_tables_for_columns,
    generate_all_sql_statements,
    create_sql_file_content,
    process_drop_tags,
    process_tag_changes,
    save_results,
    send_slack_notification
)

# Load environment variables
load_dotenv()


def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """
    Setup logging configuration

    Args:
        log_dir: Directory for log files
    """
    Path(log_dir).mkdir(exist_ok=True)

    log_file = Path(log_dir) / f"catalog_to_snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # File handler with full details
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    # Console handler with standard format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )

    return logging.getLogger(__name__)


def main():
    """Main orchestration function"""

    parser = argparse.ArgumentParser(
        description='Synchronize Catalog tags to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  FULL RUNS (Default - processes all tables with change tracking):
  # Process ALL tables (default behavior)
  python main.py

  # Force all tags as NEW (initial setup or complete refresh)
  python main.py --force-all

  LIMITED RUNS (For testing - no change tracking):
  # Process specific table by ID
  python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109

  # Process first 5 tables (for testing)
  python main.py --limit 5

  OTHER OPTIONS:
  # Custom output directory
  python main.py --output-dir ./output
        """
    )

    parser.add_argument('--table-id', type=str, help='Process a specific table by ID (limited run)')
    parser.add_argument('--table-ids', nargs='+', help='Process multiple specific table IDs (limited run)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of tables (for testing only)')
    parser.add_argument('--output-dir', type=str, default='data',
                       help='Output directory for results (default: data)')
    parser.add_argument('--sql-dir', type=str, default='sql',
                       help='Directory for SQL files (default: sql)')
    parser.add_argument('--reports-dir', type=str, default='reports',
                       help='Directory for report files (default: reports)')
    parser.add_argument('--log-dir', type=str, default='logs',
                       help='Directory for log files (default: logs)')
    parser.add_argument('--force-all', action='store_true',
                       help='Force generate all tags as NEW regardless of sync history')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.log_dir)

    # Get API credentials
    api_token = os.getenv("COALESCE_API_TOKEN")
    # Derive API URL from zone
    zone = os.getenv("COALESCE_ZONE", "US").upper()
    zone_urls = {
        "US": "https://api.us.castordoc.com/public/graphql",
        "EU": "https://api.castordoc.com/public/graphql",
    }
    api_url = zone_urls.get(zone, zone_urls["US"])

    # Clean token
    if api_token:
        api_token = api_token.strip('"').strip("'")

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    start_time = datetime.now()

    if not api_token:
        logger.error("API token not found. Please set COALESCE_API_TOKEN environment variable.")
        return 1

    slack_stats = {}
    error_msg = None

    try:
        logger.info("=" * 60)
        logger.info("COALESCE CATALOG TO SNOWFLAKE SYNC")
        logger.info("=" * 60)
        logger.info(f"Start time: {datetime.now().isoformat()}")

        # Initialize API client
        client = CatalogAPIClient(api_token, api_url)

        # Step 1: Get Snowflake warehouse IDs
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 1: Fetching Snowflake warehouses")
        logger.info("=" * 60)

        warehouse_ids = get_snowflake_warehouse_ids(client)

        if not warehouse_ids:
            error_msg = "No Snowflake warehouses found"
            logger.error(error_msg)
            return 1

        # Step 2: Fetch Snowflake tables
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 2: Fetching Snowflake tables from catalog")
        logger.info("=" * 60)

        # Skip initial table fetch when specific table ID is provided
        if args.table_id:
            logger.info(f"Skipping initial table fetch - will process specific table ID: {args.table_id}")
            snowflake_tables = []
        else:
            limit = args.limit  # None = all tables (default)
            snowflake_tables = get_all_snowflake_tables(client, warehouse_ids, limit=limit)

            if not snowflake_tables:
                error_msg = "No Snowflake tables found"
                logger.error(error_msg)
                return 1

        # Step 3: Process tables and fetch columns
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 3: Fetching columns for tables")
        logger.info("=" * 60)

        # Special handling for single table ID
        if args.table_id:
            logger.info(f"Fetching specific table metadata for ID: {args.table_id}")

            # Fetch the specific table metadata
            table_data = fetch_table_by_id(client, args.table_id)

            if table_data:
                # Use the fetched table data
                snowflake_tables = [table_data]
                table_ids = [args.table_id]
                process_limit = 1
            else:
                # If table not found, still try to process columns directly
                logger.info("Table not found in catalog, will attempt to fetch columns directly")
                snowflake_tables = []
                table_ids = [args.table_id]
                process_limit = 1
        else:
            # Log batch mode settings
            batch_size = 1000  # Fixed batch size
            logger.info(f"Using batch mode with batch size: {batch_size}")

            # Determine which tables to process
            table_ids = None
            process_limit = args.limit if args.limit else len(snowflake_tables)

            if args.table_ids:
                table_ids = args.table_ids
                process_limit = len(args.table_ids)

        catalog_columns = process_tables_for_columns(
            client,
            snowflake_tables,
            table_ids=table_ids,
            limit=process_limit,
            batch_size=1000,  # Fixed batch size
            use_batch=True    # Always use batch mode
        )

        if not catalog_columns:
            logger.warning("No tables with tagged columns found")
            sql_statements = ""
        else:
            # Step 4: Generate SQL statements
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 4: Generating SQL statements")
            logger.info("=" * 60)

            sql_statements_list = generate_all_sql_statements(catalog_columns)
            sql_statements = create_sql_file_content(sql_statements_list, catalog_columns)

        # Step 5: Generate tag change statements (unified or separate based on preference)
        unified_sql_content = ""
        drop_sql_content = ""
        new_sql_content = ""
        modified_sql_content = ""
        tag_stats = {}
        drop_count = 0

        # Full run = no table-id, no table-ids, and no limit (processing everything)
        is_full_run = not args.table_id and not args.table_ids and not args.limit

        if is_full_run and args.force_all:
            # Force all tags as NEW, skip comparison
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 5: Force generating all tags as NEW (--force-all)")
            logger.info("=" * 60)
            logger.info("Treating all current tags as NEW - no comparison with previous runs")

            # Generate all tags as NEW tags in unified format
            from catalog_to_snowflake.compute_changes import create_new_tags_sql
            new_sql_content = create_new_tags_sql(catalog_columns, previous_timestamp="N/A (forced)")
            # Create unified content with just the NEW tags
            unified_sql_content = new_sql_content
            modified_sql_content = ""
            drop_sql_content = ""
            tag_stats = {
                'new_tables': len(catalog_columns),
                'new_columns': sum(len(data.get('columns', [])) for data in catalog_columns.values()),
                'modified_tables': 0,
                'modified_columns': 0,
                'removed_table_tags': 0,
                'removed_column_tags': 0
            }

            logger.info(f"Forcing ALL tags as NEW: {tag_stats['new_tables']} tables, {tag_stats['new_columns']} columns")

        elif is_full_run:
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 5: Analyzing tag changes (new, modified, removed)")
            logger.info("=" * 60)
            logger.info("Full run detected - generating changes since last full run")

            # Generate unified SQL file with DROP first, then SET statements
            from catalog_to_snowflake.compute_changes import generate_unified_change_sql
            unified_sql_content, tag_stats = generate_unified_change_sql(
                catalog_columns, args.output_dir
            )

            # If no previous run (first run), don't generate changes file
            if not unified_sql_content:
                logger.info("First run detected - no previous data to compare against")
                logger.info("Generating only the complete current state file")
                tag_stats = {}
            # Log statistics
            elif tag_stats:
                if tag_stats.get('new_tables') > 0 or tag_stats.get('new_columns') > 0:
                    logger.info(f"NEW: {tag_stats['new_tables']} tables, {tag_stats['new_columns']} columns")
                if tag_stats.get('modified_tables') > 0 or tag_stats.get('modified_columns') > 0:
                    logger.info(f"MODIFIED: {tag_stats['modified_tables']} tables, {tag_stats['modified_columns']} columns")
                if tag_stats.get('removed_table_tags') > 0 or tag_stats.get('removed_column_tags') > 0:
                    logger.info(f"REMOVED: {tag_stats['removed_table_tags']} tables, {tag_stats['removed_column_tags']} columns")

            # Count DROP statements in unified content
            if unified_sql_content:
                drop_lines = unified_sql_content.split('\n')
                drop_count = len([s for s in drop_lines if "UNSET TAG" in s])
        elif not is_full_run:
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 5: Skipping change analysis (limited run)")
            logger.info("=" * 60)
            logger.info("Change tracking only available for full runs (no --limit, --table-id, or --table-ids)")
            logger.info("Run without limits to enable change tracking")

        # Step 6: Save results
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 6: Saving results")
        logger.info("=" * 60)

        output_files = save_results(
            snowflake_tables,
            catalog_columns,
            sql_statements,
            output_dir=args.output_dir,
            sql_dir=args.sql_dir,
            reports_dir=args.reports_dir,
            unified_sql_content=unified_sql_content,
            tag_stats=tag_stats
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Tables found: {len(snowflake_tables)}")
        logger.info(f"Tables with tagged columns: {len(catalog_columns)}")

        if sql_statements:
            sql_lines = sql_statements.split('\n')
            sql_count = len([s for s in sql_lines if s.strip() and not s.startswith('--')])
            logger.info(f"SQL statements generated: {sql_count}")

        if drop_count > 0:
            logger.info(f"DROP TAG statements generated: {drop_count}")

        # Final summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("EXECUTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"End time: {datetime.now().isoformat()}")
        logger.info("")
        logger.info("Output files:")
        for key, filepath in output_files.items():
            if filepath:
                logger.info(f"  • {key}: {filepath}")

        logger.info("")
        logger.info("✅ Synchronization completed successfully!")

        # Print SQL file locations prominently
        if output_files.get("sql_file"):
            logger.info("")
            logger.info(f"🔧 Complete current state SQL ready: {output_files['sql_file']}")
            logger.info("   Run this SQL file in Snowflake to apply ALL current tags from catalog")

        # Show the changes file if generated
        if output_files.get("unified_sql_file"):
            logger.info("")
            logger.info(f"📋 Changes since last full run ready: {output_files['unified_sql_file']}")
            logger.info("   Contains DROP statements first (cleanup), then SET statements (updates)")
            logger.info("   Review DROP statements carefully before executing")

        # Collect stats for Slack
        slack_stats['tables_found'] = len(snowflake_tables)
        slack_stats['tables_with_tags'] = len(catalog_columns)
        if sql_statements:
            sql_lines = sql_statements.split('\n')
            slack_stats['sql_statements'] = len([s for s in sql_lines if s.strip() and not s.startswith('--')])
        if drop_count > 0:
            slack_stats['drop_statements'] = drop_count
        if tag_stats:
            slack_stats.update(tag_stats)

    except Exception as e:
        logger.error(f"Execution failed: {e}")
        import traceback
        error_msg = traceback.format_exc()
        logger.error(error_msg)
        return 1

    finally:
        if slack_webhook:
            duration_sec = int((datetime.now() - start_time).total_seconds())
            m, s = divmod(duration_sec, 60)
            duration_str = f"{m:02d}:{s:02d}"
            status = "success" if error_msg is None else "failure"

            # Build GitHub Actions URL if running in CI
            actions_url = None
            gh_server = os.getenv("GITHUB_SERVER_URL")
            gh_repo = os.getenv("GITHUB_REPOSITORY")
            gh_run_id = os.getenv("GITHUB_RUN_ID")
            if gh_server and gh_repo and gh_run_id:
                actions_url = f"{gh_server}/{gh_repo}/actions/runs/{gh_run_id}"

            send_slack_notification(
                webhook_url=slack_webhook,
                status=status,
                stats=slack_stats,
                duration=duration_str,
                error=error_msg,
                actions_url=actions_url
            )

    return 0


if __name__ == "__main__":
    exit(main())