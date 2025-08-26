import struct
from azure.identity import DefaultAzureCredential
import pyodbc

class FastMCP:
    def __init__(self, name):
        self.name = name
        self.tools = {}
    
    def tool(self):
        def decorator(func):
            self.tools[func.__name__] = func
            return func
        return decorator

def _pack_token(string: str) -> bytes:
    bytes_str = b""
    for i in string.encode("utf-8"):
        bytes_str += bytes({i})
        bytes_str += bytes(1)

    return struct.pack("=i", len(bytes_str)) + bytes_str

SQL_COPT_SS_ACCESS_TOKEN = 1256

SERVER = "s4wmuxdh4mqerkfjddraua2kni-k46ujidhbx6e5cv6qxljbxzwnq.datawarehouse.fabric.microsoft.com"
DATABASE = "cms_lakehouse"
CONNECTION_STRING = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Encrypt=yes"

credential = DefaultAzureCredential()
token = credential.get_token("https://database.windows.net//.default")

# This is needed on OSX/Linux, not sure about Windows
# https://techcommunity.microsoft.com/blog/appsonazureblog/how-to-connect-azure-sql-database-from-python-function-app-using-managed-identit/3035595
packed_access_token = _pack_token(token.token)

attrs_before={
    SQL_COPT_SS_ACCESS_TOKEN: packed_access_token
}

mcp = FastMCP("Fabric MCP Server Demo")


# MCP tool to get table schema info
@mcp.tool()
def get_table_info() -> str:
    """Get column information from the CMS provider drug costs table."""
    with pyodbc.connect(CONNECTION_STRING, attrs_before=attrs_before) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 * FROM [dbo].[cms_provider_drug_costs]")
        columns = [column[0] for column in cursor.description]
        return "Table columns:\n" + "\n".join(columns)

@mcp.tool()
def get_top_prescribers(top: int = 10) -> str:
    """Fetch top N prescribers by total claims with their location and prescriber type."""
    with pyodbc.connect(CONNECTION_STRING, attrs_before=attrs_before) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT TOP ({top}) 
                [Prscrbr_Full_Name],
                [Prscrbr_City],
                [Prscrbr_State_Abrvtn],
                [Prscrbr_Type],
                SUM([Tot_Clms]) as Total_Claims,
                SUM([Tot_Drug_Cst]) as Total_Cost,
                COUNT(DISTINCT [Brnd_Name]) as Unique_Brands
            FROM [dbo].[cms_provider_drug_costs] 
            GROUP BY [Prscrbr_Full_Name], [Prscrbr_City], [Prscrbr_State_Abrvtn], [Prscrbr_Type]
            ORDER BY Total_Claims DESC;
        """)
        rows = cursor.fetchall()
        return f"Returning top {top} prescribers by total claims:\n" + "\n".join(
            f"{row[0]} ({row[3]}) - {row[1]}, {row[2]} - {row[4]:,} claims, ${row[5]:,.2f} total cost, {row[6]} unique brands" 
            for row in rows
        )

@mcp.tool()
def get_top_states(top: int = 10) -> str:
    """Fetch top N states by total claims with beneficiary metrics."""
    with pyodbc.connect(CONNECTION_STRING, attrs_before=attrs_before) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT TOP ({top})
                [Prscrbr_State_Abrvtn] as State,
                COUNT(DISTINCT [Prscrbr_Full_Name]) as Unique_Prescribers,
                SUM([Tot_Clms]) as Total_Claims,
                SUM([Tot_Drug_Cst]) as Total_Cost,
                SUM([Tot_Benes]) as Total_Beneficiary_Events,
                SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(SUM([Tot_Clms]), 0) as Cost_Per_Claim,
                SUM([Tot_Clms]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Claims_Per_Prescriber,
                SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Cost_Per_Prescriber
            FROM [dbo].[cms_provider_drug_costs]
            GROUP BY [Prscrbr_State_Abrvtn]
            ORDER BY Total_Claims DESC;
        """)
        rows = cursor.fetchall()
        def format_number(value, format_spec, default=0):
            if value is None:
                value = default
            return f"{value:{format_spec}}"

        return f"Returning top {top} states by total claims:\n" + "\n".join(
            f"{row[0]} - {row[1]:,} prescribers, {row[2]:,} claims, ${row[3]:,.2f} total cost\n" +
            f"    Cost per claim: ${format_number(row[5], '.2f')}, " +
            f"Claims per prescriber: {format_number(row[6], '.1f')}, " +
            f"Cost per prescriber: ${format_number(row[7], ',.2f')}\n" +
            f"    Total beneficiary events: {row[4]:,} (Note: May include repeat visits)"
            for row in rows
        )

@mcp.tool()
def get_top_cities(top: int = 20) -> str:
    """Fetch top N cities by total claims with efficiency metrics."""
    with pyodbc.connect(CONNECTION_STRING, attrs_before=attrs_before) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT TOP ({top})
                [Prscrbr_City],
                [Prscrbr_State_Abrvtn],
                COUNT(DISTINCT [Prscrbr_Full_Name]) as Unique_Prescribers,
                SUM([Tot_Clms]) as Total_Claims,
                SUM([Tot_Drug_Cst]) as Total_Cost,
                SUM([Tot_Benes]) as Total_Beneficiary_Events,
                SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(SUM([Tot_Clms]), 0) as Cost_Per_Claim,
                SUM([Tot_Clms]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Claims_Per_Prescriber,
                SUM([Tot_Drug_Cst]) * 1.0 / NULLIF(COUNT(DISTINCT [Prscrbr_Full_Name]), 0) as Cost_Per_Prescriber
            FROM [dbo].[cms_provider_drug_costs]
            GROUP BY [Prscrbr_City], [Prscrbr_State_Abrvtn]
            ORDER BY Total_Claims DESC;
        """)
        rows = cursor.fetchall()
        def format_number(value, format_spec, default=0):
            if value is None:
                value = default
            return f"{value:{format_spec}}"

        return f"Returning top {top} cities by total claims:\n" + "\n".join(
            f"{row[0]}, {row[1]} - {row[2]:,} prescribers, {row[3]:,} claims, ${row[4]:,.2f} total cost\n" +
            f"    Cost per claim: ${format_number(row[6], '.2f')}, " +
            f"Claims per prescriber: {format_number(row[7], '.1f')}, " +
            f"Cost per prescriber: ${format_number(row[8], ',.2f')}\n" +
            f"    Total beneficiary events: {row[5]:,} (Note: May include repeat visits)"
            for row in rows
        )

@mcp.tool()
def get_prescriber_types_summary() -> str:
    """Get a summary of prescriber types with efficiency metrics."""
    with pyodbc.connect(CONNECTION_STRING, attrs_before=attrs_before) as conn:
        cursor = conn.cursor()
        cursor.execute("""
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
            FROM [dbo].[cms_provider_drug_costs]
            GROUP BY [Prscrbr_Type]
            ORDER BY Total_Claims DESC;
        """)
        rows = cursor.fetchall()
        def format_number(value, format_spec, default=0):
            if value is None:
                value = default
            return f"{value:{format_spec}}"

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


if __name__ == "__main__":
    # Test all tools
    print("\nTop Prescribers:")
    print(get_top_prescribers(10))
    
    print("\nTop States:")
    print(get_top_states(10))
    
    print("\nTop Cities:")
    print(get_top_cities(20))
    
    print("\nPrescriber Types Summary:")
    print(get_prescriber_types_summary())