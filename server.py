import os
import struct
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
import pyodbc
from abc import ABC, abstractmethod
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class DatabaseConnection(ABC):
    """Abstract base class for database connections"""
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_query(self, query, params=None):
        pass

class FastMCP:
    """Model Context Protocol Server Implementation"""
    def __init__(self, name):
        self.name = name
        self.tools = {}
    
    def tool(self):
        def decorator(func):
            self.tools[func.__name__] = func
            return func
        return decorator

# Initialize MCP server

class AzureSQLConnection(DatabaseConnection):
    """Azure SQL Database connection implementation"""
    def __init__(self):
        self.connection = None
        self._setup_connection()

    def _setup_connection(self):
        """Set up the database connection parameters"""
        # Get configuration from environment variables
        self.server = os.getenv('DB_SERVER')
        self.database = os.getenv('DB_NAME')
        self.table = os.getenv('DB_TABLE')

        if not all([self.server, self.database, self.table]):
            raise ValueError("Required environment variables are not set")

        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Encrypt=yes"
        )

    def connect(self):
        """Establish database connection with Azure managed identity"""
        try:
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net//.default")
            
            # Pack the access token
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            packed_token = self._pack_token(token.token)
            
            # Connect with the token
            self.connection = pyodbc.connect(
                self.connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: packed_token}
            )
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise

    @staticmethod
    def _pack_token(token: str) -> bytes:
        """Pack the access token for SQL Server"""
        bytes_str = b""
        for i in token.encode("utf-8"):
            bytes_str += bytes({i})
            bytes_str += bytes(1)
        return struct.pack("=i", len(bytes_str)) + bytes_str


def initialize_mcp():
    """Initialize the MCP server with configured tools"""
    mcp = FastMCP("Prescriber Analytics MCP")
    db = AzureSQLConnection()
    
    @mcp.tool()
    def get_schema_info() -> str:
        """Get database schema information"""
        try:
            results = db.execute_query("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?", [db.table])
            return "Schema:\n" + "\n".join(f"{col[0]}: {col[1]}" for col in results)
        except Exception as e:
            return f"Error retrieving schema: {str(e)}"

    return mcp


if __name__ == "__main__":
    try:
        mcp = initialize_mcp()
        logger.info("MCP server initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MCP server: {str(e)}")