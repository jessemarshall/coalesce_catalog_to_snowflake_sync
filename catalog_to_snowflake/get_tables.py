#!/usr/bin/env python3
"""
Module to fetch Snowflake tables from Coalesce Catalog
"""

import logging
from typing import List, Dict, Optional
from .catalog_api_client import CatalogAPIClient

logger = logging.getLogger(__name__)


def fetch_snowflake_tables(client: CatalogAPIClient, warehouse_id: str, limit: int = 1000, page: int = 0) -> Dict:
    """
    Fetch Snowflake tables for a specific warehouse

    Args:
        client: CatalogAPIClient instance
        warehouse_id: Snowflake warehouse ID
        limit: Number of tables to fetch per page
        page: Page number (0-based)

    Returns:
        Dictionary with tables data and metadata
    """
    query = """
    query GetTablesForWarehouse($warehouseId: String!, $limit: Int!, $page: Int!) {
        getTables(
            scope: {
                warehouseId: $warehouseId
            }
            pagination: {
                nbPerPage: $limit
                page: $page
            }
        ) {
            totalCount
            data {
                id
                name
                tableType
                createdAt
                updatedAt
                deletedAt
                schema {
                    id
                    name
                    database {
                        id
                        name
                        warehouse {
                            id
                            name
                        }
                    }
                }
                tagEntities {
                    id
                    origin
                    createdAt
                    updatedAt
                    tag {
                        id
                        label
                    }
                }
            }
        }
    }
    """

    variables = {
        "warehouseId": warehouse_id,
        "limit": limit,
        "page": page
    }

    try:
        result = client.execute_query(query, variables)
        return result.get("data", {}).get("getTables", {})

    except Exception as e:
        logger.error(f"Failed to fetch tables for warehouse {warehouse_id}: {e}")
        return {}


def fetch_table_by_id(client: CatalogAPIClient, table_id: str) -> Optional[Dict]:
    """
    Fetch a specific table by its ID

    Args:
        client: CatalogAPIClient instance
        table_id: The specific table ID to fetch

    Returns:
        Table object if found, None otherwise
    """
    query = """
    query GetTableById($tableIds: [String!], $page: Int!, $pageSize: Int!) {
        getTables(
            scope: {
                ids: $tableIds
            }
            pagination: {
                nbPerPage: $pageSize
                page: $page
            }
        ) {
            totalCount
            data {
                id
                name
                tableType
                createdAt
                updatedAt
                deletedAt
                schema {
                    id
                    name
                    database {
                        id
                        name
                        warehouse {
                            id
                            name
                        }
                    }
                }
                tagEntities {
                    id
                    origin
                    createdAt
                    updatedAt
                    tag {
                        id
                        label
                    }
                }
            }
        }
    }
    """

    variables = {
        "tableIds": [table_id],
        "page": 0,
        "pageSize": 1
    }

    try:
        result = client.execute_query(query, variables)
        tables_data = result.get("data", {}).get("getTables", {})
        tables = tables_data.get("data", [])

        if tables and len(tables) > 0:
            table_data = tables[0]
            logger.info(f"✓ Found table: {table_data.get('schema', {}).get('database', {}).get('name')}.{table_data.get('schema', {}).get('name')}.{table_data.get('name')}")
            return table_data
        else:
            logger.warning(f"Table with ID {table_id} not found in catalog")
            return None
    except Exception as e:
        logger.error(f"Failed to fetch table {table_id}: {e}")
        return None


def get_all_snowflake_tables(client: CatalogAPIClient, warehouse_ids: List[str], limit: Optional[int] = 1000) -> List[Dict]:
    """
    Get all Snowflake tables from the given warehouse IDs

    Args:
        client: CatalogAPIClient instance
        warehouse_ids: List of warehouse IDs
        limit: Maximum number of tables to fetch (None for all tables)

    Returns:
        List of table objects
    """
    if not warehouse_ids:
        logger.warning("No warehouse IDs provided")
        return []

    tables: List[Dict] = []
    total_available = 0
    seen_table_ids = set()

    for warehouse_index, warehouse_id in enumerate(warehouse_ids, 1):
        logger.info(f"Fetching tables from warehouse {warehouse_index}/{len(warehouse_ids)}: {warehouse_id}")

        if limit is None:
            page = 0
            page_size = 1000

            while True:
                tables_data = fetch_snowflake_tables(client, warehouse_id, limit=page_size, page=page)
                warehouse_tables = tables_data.get("data", [])
                warehouse_total = tables_data.get("totalCount", 0)

                if page == 0:
                    total_available += warehouse_total
                    logger.info(f"  Total tables available in warehouse: {warehouse_total}")

                new_tables = 0
                for table in warehouse_tables:
                    table_id = table.get("id")
                    if table_id and table_id not in seen_table_ids:
                        seen_table_ids.add(table_id)
                        tables.append(table)
                        new_tables += 1

                logger.info(
                    f"  Fetched page {page + 1}: {len(warehouse_tables)} tables "
                    f"({new_tables} new, total so far: {len(tables)})"
                )

                if len(warehouse_tables) < page_size:
                    break

                page += 1
        else:
            remaining = limit - len(tables)
            if remaining <= 0:
                break

            tables_data = fetch_snowflake_tables(client, warehouse_id, limit=remaining)
            warehouse_tables = tables_data.get("data", [])
            warehouse_total = tables_data.get("totalCount", 0)
            total_available += warehouse_total

            new_tables = 0
            for table in warehouse_tables:
                table_id = table.get("id")
                if table_id and table_id not in seen_table_ids:
                    seen_table_ids.add(table_id)
                    tables.append(table)
                    new_tables += 1
                    if len(tables) >= limit:
                        break

            logger.info(
                f"  Fetched up to {remaining} tables from warehouse "
                f"({len(warehouse_tables)} returned, {new_tables} new, total so far: {len(tables)})"
            )

    logger.info(f"✅ Found {len(tables)} Snowflake tables (Total available across warehouses: {total_available})")

    if tables:
        logger.info("")
        logger.info("Sample tables found:")
        for i, table in enumerate(tables[:5], 1):
            schema = table.get("schema", {})
            database = schema.get("database", {})
            full_name = f"{database.get('name')}.{schema.get('name')}.{table.get('name')}"
            logger.info(f"  {i}. {full_name} (ID: {table.get('id')})")

        if len(tables) > 5:
            logger.info(f"  ... and {len(tables) - 5} more tables")

    return tables
