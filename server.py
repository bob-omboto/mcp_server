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


def format_number(value, format_spec, default=0):
    """Helper function to format numbers with proper formatting"""
    if value is None:
        value = default
    return f"{value:{format_spec}}"

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

    @mcp.tool()
    def get_top_prescribers(top: int = 10) -> str:
        """Fetch top N prescribers by total claims with their location and prescriber type."""
        try:
            query = f"""
                SELECT TOP (?)
                    [Prscrbr_Full_Name],
                    [Prscrbr_City],
                    [Prscrbr_State_Abrvtn],
                    [Prscrbr_Type],
                    SUM([Tot_Clms]) as Total_Claims,
                    SUM([Tot_Drug_Cst]) as Total_Cost,
                    COUNT(DISTINCT [Brnd_Name]) as Unique_Brands
                FROM [dbo].[{db.table}]
                GROUP BY [Prscrbr_Full_Name], [Prscrbr_City], [Prscrbr_State_Abrvtn], [Prscrbr_Type]
                ORDER BY Total_Claims DESC;
            """
            rows = db.execute_query(query, [top])
            return f"Returning top {top} prescribers by total claims:\n" + "\n".join(
                f"{row[0]} ({row[3]}) - {row[1]}, {row[2]} - {row[4]:,} claims, ${row[5]:,.2f} total cost, {row[6]} unique brands" 
                for row in rows
            )
        except Exception as e:
            return f"Error retrieving top prescribers: {str(e)}"

    @mcp.tool()
    def get_top_states(top: int = 10) -> str:
        """Fetch top N states by total claims with beneficiary metrics."""
        try:
            query = f"""
                SELECT TOP (?)
                    [Prscrbr_State_Abrvtn] as State,
                    COUNT(DISTINCT [Prscrbr_Full_Name]) as Unique_Prescribers,
                    SUM([Tot_Clms]) as Total_Claims,
                    SUM([Tot_Drug_Cst]) as Total_Cost,
                    SUM([Tot_Benes]) as Total_Beneficiary_Events,
                    SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(SUM([Tot_Clms]), 0) as Cost_Per_Claim,
                    SUM([Tot_Clms]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Claims_Per_Prescriber,
                    SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Cost_Per_Prescriber
                FROM [dbo].[{db.table}]
                GROUP BY [Prscrbr_State_Abrvtn]
                ORDER BY Total_Claims DESC;
            """
            rows = db.execute_query(query, [top])
            return f"Returning top {top} states by total claims:\n" + "\n".join(
                f"{row[0]} - {row[1]:,} prescribers, {row[2]:,} claims, ${row[3]:,.2f} total cost\n" +
                f"    Cost per claim: ${format_number(row[5], '.2f')}, " +
                f"Claims per prescriber: {format_number(row[6], '.1f')}, " +
                f"Cost per prescriber: ${format_number(row[7], ',.2f')}\n" +
                f"    Total beneficiary events: {row[4]:,} (Note: May include repeat visits)"
                for row in rows
            )
        except Exception as e:
            return f"Error retrieving top states: {str(e)}"

    @mcp.tool()
    def get_prescriber_types_summary() -> str:
        """Get a summary of prescriber types with efficiency metrics."""
        try:
            query = f"""
                SELECT 
                    [Prscrbr_Type],
                    COUNT(DISTINCT [Prscrbr_Full_Name]) as Unique_Prescribers,
                    SUM([Tot_Clms]) as Total_Claims,
                    SUM([Tot_Drug_Cst]) as Total_Cost,
                    SUM([Tot_Benes]) as Total_Beneficiary_Events,
                    SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(SUM([Tot_Clms]), 0) as Cost_Per_Claim,
                    SUM([Tot_Clms]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Claims_Per_Prescriber,
                    SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Cost_Per_Prescriber,
                    COUNT(DISTINCT [Brnd_Name]) as Unique_Brands,
                    SUM([Tot_Day_Suply]) * 1.0 / NULLIF(SUM([Tot_Clms]), 0) as Avg_Days_Per_Claim
                FROM [dbo].[{db.table}]
                GROUP BY [Prscrbr_Type]
                ORDER BY Total_Claims DESC;
            """
            rows = db.execute_query(query)
            return "Summary of prescriber types:\n" + "\n".join(
                f"{row[0]}: {format_number(row[1], ',', 0)} prescribers, {format_number(row[2], ',', 0)} claims, ${format_number(row[3], ',.2f', 0)} total cost\n" +
                f"    Cost per claim: ${format_number(row[5], '.2f')}, " +
                f"Claims per prescriber: {format_number(row[6], '.1f')}, " +
                f"Cost per prescriber: ${format_number(row[7], ',.2f')}\n" +
                f"    {format_number(row[8], '', 0)} unique brands prescribed, " +
                f"{format_number(row[9], '.1f')} days supply per claim\n" +
                f"    Total beneficiary events: {format_number(row[4], ',', 0)} (Note: May include repeat visits)"
                for row in rows
            )
        except Exception as e:
            return f"Error retrieving prescriber types summary: {str(e)}"

    return mcp


if __name__ == "__main__":
    try:
        mcp = initialize_mcp()
        logger.info("MCP server initialized successfully")
        
        # Test the analytics functions
        logger.info("\nRunning prescriber analytics...")
        print("\nPrescriber Types Summary:")
        print(mcp.tools["get_prescriber_types_summary"]())
        
        print("\nTop States:")
        print(mcp.tools["get_top_states"](10))
        
        print("\nTop Prescribers:")
        print(mcp.tools["get_top_prescribers"](10))
    except Exception as e:
        logger.error(f"Failed to initialize MCP server: {str(e)}")