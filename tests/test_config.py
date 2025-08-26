import os
import pytest
from server import AzureSQLConnection

def test_database_configuration():
    """Test that all required database configuration is available"""
    try:
        db = AzureSQLConnection()
        assert db.server is not None, "DB_SERVER environment variable is not set"
        assert db.database is not None, "DB_NAME environment variable is not set"
        assert db.table is not None, "DB_TABLE environment variable is not set"
    except ValueError as e:
        pytest.fail(f"Database configuration error: {str(e)}")
