from fastapi import FastAPI, HTTPException
from server import initialize_mcp
import uvicorn
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="MCP Analytics API")
mcp = initialize_mcp()

@app.get("/")
async def root():
    """Root endpoint returning API information"""
    return {
        "name": "MCP Analytics API",
        "version": "1.0.0",
        "endpoints": [
            "/schema",
            "/prescriber-types",
            "/top-prescribers",
            "/top-states"
        ]
    }

@app.get("/schema")
async def get_schema():
    """Get database schema information"""
    try:
        return {"schema": mcp.tools["get_schema_info"]()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/prescriber-types")
async def get_prescriber_types():
    """Get summary of prescriber types with efficiency metrics"""
    try:
        return {"data": mcp.tools["get_prescriber_types_summary"]()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/top-prescribers")
async def get_top_prescribers(limit: Optional[int] = 10):
    """Get top N prescribers by total claims"""
    try:
        return {"data": mcp.tools["get_top_prescribers"](limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/top-states")
async def get_top_states(limit: Optional[int] = 10):
    """Get top N states by total claims"""
    try:
        return {"data": mcp.tools["get_top_states"](limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
