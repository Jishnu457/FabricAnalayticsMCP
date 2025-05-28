from fastapi import FastAPI, HTTPException
from mcp.server.fastmcp import FastMCP
from fastapi.middleware.cors import CORSMiddleware
from openai import AzureOpenAI
import os
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel
import uvicorn
import pyodbc
from typing import List, Dict, Any
import json
import re
 
# Load environment variables
load_dotenv()
 
# FastAPI app
app = FastAPI(title="Microsoft Fabric SQL Analytics MCP Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
 
# MCP server
mcp = FastMCP("Fabric SQL Analytics", dependencies=["pyodbc", "fastapi", "python-dotenv", "pandas"])
 
# Azure OpenAI client
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2023-05-15"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)
 
# --- Core Functions ---
 
def get_fabric_connection():
    """Create connection to Microsoft Fabric SQL Database"""
    server = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_DATABASE")
    client_id = os.getenv("FABRIC_CLIENT_ID")
    client_secret = os.getenv("FABRIC_CLIENT_SECRET")
    tenant_id = os.getenv("FABRIC_TENANT_ID")
   
    if not all([server, database, client_id, client_secret, tenant_id]):
        raise Exception("Missing required environment variables: FABRIC_SQL_ENDPOINT, FABRIC_DATABASE, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID")
   
    connection_string = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={client_id};"
        f"PWD={client_secret};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
   
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        print(f"Connected to database: {database}")  # Debug
        return conn
    except Exception as e:
        print(f"Connection failed: {str(e)}")  # Debug
        raise
 
def execute_query(query: str, params=None) -> List[Dict[str, Any]]:
    """Execute query and return results"""
    conn = get_fabric_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params) if params else cursor.execute(query)
       
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
       
        result = [{columns[i]: value for i, value in enumerate(row)} for row in rows]
       
        print(f"Query executed: {query[:100]}... Rows returned: {len(result)}")  # Debug
        return result
    except Exception as e:
        print(f"Query failed: {str(e)}")  # Debug
        raise
    finally:
        cursor.close()
        conn.close()
 
def clean_generated_sql(sql_text: str) -> str:
    """Clean SQL from LLM response"""
    sql = sql_text.strip()
   
    if sql.startswith("```"):
        lines = sql.split('\n')
        sql = '\n'.join(lines[1:-1]) if len(lines) > 2 else sql.replace("```", "")
   
    prefixes = ["```sql", "sql:", "SQL:", "Query:"]
    for prefix in prefixes:
        if sql.lower().startswith(prefix.lower()):
            sql = sql[len(prefix):].strip()
   
    sql = sql.strip().rstrip('`')
   
    sql_upper = sql.upper().strip()
    if sql_upper.startswith('TOP ') and 'SELECT' not in sql_upper[:10]:
        sql = 'SELECT ' + sql
    elif re.match(r'^[A-Za-z_\[\]]+.*FROM\s+', sql, re.IGNORECASE) and not sql_upper.startswith('SELECT'):
        sql = 'SELECT ' + sql
   
    return sql
 
def ask_llm(prompt: str) -> str:
    """Send prompt to Azure OpenAI"""
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not deployment:
        raise ValueError("AZURE_OPENAI_DEPLOYMENT not set")
   
    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": "You are a Microsoft Fabric SQL Database expert. Always start queries with SELECT and use [schema].[Table Name] for tables."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=800
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"LLM request failed: {str(e)}")  # Debug
        raise
 
# --- Table Management ---
 
@mcp.resource("data://tables")
def list_fabric_tables():
    """List tables in Fabric SQL Database"""
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND TABLE_NAME NOT LIKE 'sys%'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
   
    results = execute_query(query)
    tables = []
    for row in results:
        schema = row["TABLE_SCHEMA"]
        table_name = row["TABLE_NAME"]
        full_name = f"{schema}.{table_name}"
       
        try:
            test_query = f"SELECT TOP 1 * FROM [{schema}].[{table_name}]"
            execute_query(test_query)
            working_format = f"[{schema}].[{table_name}]"
        except Exception as e:
            print(f"Table {full_name} not accessible: {str(e)}")  # Debug
            working_format = None
       
        tables.append({
            "schema": schema,
            "name": table_name,
            "type": row["TABLE_TYPE"],
            "full_name": full_name,
            "working_format": working_format or f"[{schema}].[{table_name}]"
        })
   
    print(f"Found {len(tables)} tables")  # Debug
    return tables
 
def get_table_schema(table_name: str):
    """Get schema for a table"""
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        all_tables = list_fabric_tables()
        matching = [t for t in all_tables if t["name"].lower() == table_name.lower()]
        if matching:
            schema, table = matching[0]["schema"], matching[0]["name"]
        else:
            schema, table = "dbo", table_name
   
    query = """
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
   
    try:
        results = execute_query(query, (schema, table))
        column_names = [col["COLUMN_NAME"] for col in results]
        print(f"Columns in {schema}.{table}: {column_names}")  # Debug
        return {"table_name": f"{schema}.{table}", "columns": results}
    except Exception as e:
        print(f"Failed to get schema for {schema}.{table}: {str(e)}")  # Debug
        raise
 
def get_tables_info():
    """Get all tables with their columns"""
    all_tables = list_fabric_tables()
    tables_info = []
   
    for table in all_tables:
        try:
            schema_info = get_table_schema(table["full_name"])
            columns = [f"[{col['COLUMN_NAME']}] ({col['DATA_TYPE']})" for col in schema_info['columns']]
            table_info = f"Table: [{table['schema']}].[{table['name']}]\nColumns: {', '.join(columns)}"
            tables_info.append(table_info)
        except Exception as e:
            print(f"Skipping schema for {table['full_name']}: {str(e)}")  # Debug
            tables_info.append(f"Table: [{table['schema']}].[{table['name']}] (Schema unavailable)")
   
    return "\n\n".join(tables_info)
 
def build_smart_prompt(question: str, tables_info: str) -> str:
    """Build prompt for smart SQL generation"""
    sample_data = ""
    try:
        sample_query = "SELECT TOP 5 * FROM [dbo].[ThreatsDetectedTable]"
        sample_results = execute_query(sample_query)
        sample_data = f"\nSample Data for [dbo].[ThreatsDetectedTable]:\n{json.dumps(sample_results, indent=2, default=str)}"
    except Exception as e:
        sample_data = "\nSample Data for [dbo].[ThreatsDetectedTable]: Not available"
   
    severity_info = ""
    try:
        severity_query = "SELECT DISTINCT [Severity] FROM [dbo].[ThreatsDetectedTable]"
        severity_results = execute_query(severity_query)
        severity_values = [row["Severity"] for row in severity_results if row["Severity"] is not None]
        severity_info = f"\nSeverity Values: {', '.join([f'{v!r}' for v in severity_values])} (case-sensitive)"
    except Exception as e:
        severity_info = "\nSeverity Values: Not available"
   
    return f"""
Generate a Microsoft Fabric SQL Database query to answer this question.
 
CRITICAL RULES for Fabric SQL Database:
1. ALWAYS start with "SELECT"
2. ALWAYS use [schema].[Table Name] format for tables (e.g., [dbo].[ThreatsDetectedTable])
3. Use exact column names as listed, with brackets for names with spaces (e.g., [Employee ID])
4. Include TOP 50 for performance
5. Return only the SQL query, no explanations
6. If impossible, return exactly: INSUFFICIENT_DATA
7. Do NOT omit schema prefix (e.g., use [dbo].[Table Name], not [Table Name])
8. For string comparisons (e.g., Severity), use exact case as shown in sample data or Severity Values
 
Available Tables:
{tables_info}
 
{sample_data}
 
{severity_info}
 
Question: {question}
 
Examples:
- SELECT TOP 50 [Employee ID], [Threat Type], [Severity] FROM [dbo].[ThreatsDetectedTable] WHERE [Severity] = 'high'
- SELECT TOP 50 * FROM [dbo].[ResourceCost] WHERE Cost > 100
- SELECT TOP 50 [t1].[Employee ID], [t2].[Department] FROM [dbo].[Employees] [t1] JOIN [dbo].[Departments] [t2] ON [t1].[Department ID] = [t2].[Department ID]
 
Return only the complete SQL query starting with SELECT:
"""
 
# --- Pydantic Models ---
 
class SmartQueryRequest(BaseModel):
    question: str
    limit: int = 50
 
# --- MCP Tools ---
 
@mcp.tool("fabric.smart_analyze")
def smart_analyze_question(question: str, limit: int = 50) -> Dict[str, Any]:
    """Smart analysis with table name validation"""
    try:
        available_tables = list_fabric_tables()
        if not available_tables:
            return {
                "question": question,
                "error": "No tables found in Fabric SQL Database",
                "suggestion": "Check your connection and table permissions"
            }
       
        tables_info = get_tables_info()
        prompt = build_smart_prompt(question, tables_info)
        generated_sql_raw = ask_llm(prompt)
        generated_sql = clean_generated_sql(generated_sql_raw)
       
        print("Generated SQL:", generated_sql)  # Debug
       
        if generated_sql.strip().upper() == "INSUFFICIENT_DATA":
            return {
                "question": question,
                "error": "Cannot answer with available tables",
                "available_tables": [t["full_name"] for t in available_tables]
            }
       
        if not generated_sql.upper().startswith("SELECT"):
            return {
                "question": question,
                "error": "Generated query is not a SELECT statement",
                "generated_sql": generated_sql
            }
       
        if "TOP" not in generated_sql.upper():
            generated_sql = generated_sql.replace("SELECT", f"SELECT TOP {limit}", 1)
       
        # Make Severity comparison case-insensitive
        if "[Severity] = 'high'" in generated_sql.lower():
            generated_sql = generated_sql.replace(
                "[Severity] = 'high'",
                "LOWER([Severity]) = 'high'"
            )
       
        all_valid_names = [f"[{t['schema']}].[{t['name']}]" for t in available_tables]
        query_lower = generated_sql.lower()
        has_valid_table = any(name.lower() in query_lower for name in all_valid_names)
       
        if not has_valid_table:
            return {
                "question": question,
                "error": "Generated query references invalid tables",
                "generated_sql": generated_sql,
                "available_tables": [t["full_name"] for t in available_tables]
            }
       
        try:
            results = execute_query(generated_sql)
        except Exception as e:
            print(f"Query execution failed: {str(e)}")  # Debug
            return {
                "question": question,
                "error": f"SQL execution failed: {str(e)}",
                "generated_sql": generated_sql,
                "available_tables": [t["full_name"] for t in available_tables],
                "suggestion": "Check table/column names and permissions"
            }
       
        if not results:
            return {
                "question": question,
                "generated_sql": generated_sql,
                "analysis": "Query executed but returned no results",
                "available_tables": [t["full_name"] for t in available_tables]
            }
       
        context = json.dumps(results[:10], indent=2, default=str)
        analysis_prompt = f"""
Based on this data, answer the question directly and concisely:
 
Data: {context}
Question: {question}
 
Provide a specific, direct answer:
"""
       
        analysis = ask_llm(analysis_prompt)
       
        return {
            "question": question,
            "generated_sql": generated_sql,
            "analysis": analysis,
            "result_count": len(results)
        }
       
    except Exception as e:
        print(f"Analysis failed: {str(e)}")  # Debug
        return {
            "question": question,
            "error": f"Analysis failed: {str(e)}",
            "suggestion": "Check connection and try a simpler question"
        }
 
# --- FastAPI Endpoints ---
 
@app.get("/")
async def root():
    return {"status": "ok", "service": "Microsoft Fabric SQL Analytics MCP Server"}
 
@app.get("/api/fabric/tables")
def get_tables():
    """Get all tables"""
    try:
        tables = list_fabric_tables()
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.post("/api/fabric/smart-analyze")
def smart_analyze_endpoint(req: SmartQueryRequest):
    """Smart analysis endpoint"""
    try:
        result = smart_analyze_question(req.question, req.limit)
       
        if "error" in result:
            raise HTTPException(status_code=400, detail=result)
       
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/fabric/health")
def health_check():
    """Simple health check"""
    try:
        execute_query("SELECT 1")
        tables = list_fabric_tables()
        return {
            "status": "healthy",
            "tables_found": len(tables),
            "connection": "ok"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/fabric/list-tables")
def list_all_tables():
    """List all available tables with details"""
    try:
        tables = list_fabric_tables()
        return {
            "tables": tables,
            "count": len(tables),
            "table_names": [t["name"] for t in tables],
            "full_names": [t["full_name"] for t in tables]
        }
    except Exception as e:
        return {"error": str(e), "suggestion": "Check your Fabric connection"}
 
@app.post("/api/fabric/direct-test")
def direct_fabric_test():
    """Test direct access to ThreatsDetectedTable"""
    test_queries = [
        "SELECT TOP 1 * FROM [dbo].[ThreatsDetectedTable]",  # Sample data
        "SELECT TOP 10 [Employee ID], [Severity] FROM [dbo].[ThreatsDetectedTable] WHERE [Severity] = 'high'",  # Check high severity
        "SELECT TOP 10 [Employee ID], [Severity] FROM [dbo].[ThreatsDetectedTable] WHERE [Employee ID] = 1152",  # Check Employee ID
        "SELECT DISTINCT [Severity] FROM [dbo].[ThreatsDetectedTable]",  # Check Severity values
        "SELECT COUNT(*) AS row_count FROM [dbo].[ThreatsDetectedTable]"  # Check total rows
    ]
   
    results = {}
    for i, query in enumerate(test_queries):
        try:
            result = execute_query(query)
            results[f"query_{i+1}"] = {
                "query": query,
                "status": "SUCCESS",
                "rows_returned": len(result),
                "first_row_keys": list(result[0].keys()) if result else [],
                "data": result[:5]  # Include first 5 rows
            }
        except Exception as e:
            results[f"query_{i+1}"] = {
                "query": query,
                "status": "FAILED",
                "error": str(e)[:200]
            }
   
    return {
        "message": "Testing access to 'ThreatsDetectedTable'",
        "results": results,
        "recommendation": "Inspect data in successful queries to verify column names and values"
    }
 
@app.get("/api/fabric/inspect-table/{table_name}")
def inspect_table(table_name: str):
    """Inspect table schema and data"""
    try:
        schema_info = get_table_schema(table_name)
        columns = [col["COLUMN_NAME"] for col in schema_info["columns"]]
       
        sample_query = f"SELECT TOP 10 * FROM [{schema_info['table_name']}]"
        sample_data = execute_query(sample_query)
       
        unique_values = {}
        for col in columns:
            try:
                unique_query = f"SELECT DISTINCT [{col}] FROM [{schema_info['table_name']}]"
                results = execute_query(unique_query)
                unique_values[col] = [row[col] for row in results if row[col] is not None]
            except Exception as e:
                unique_values[col] = f"Error: {str(e)}"
       
        return {
            "table": schema_info["table_name"],
            "columns": columns,
            "sample_data": sample_data,
            "unique_values": unique_values
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/fabric/database-info")
def database_info():
    """Get database information"""
    try:
        query = "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS Collation"
        result = execute_query(query)
        collation = result[0]["Collation"] if result else "Unknown"
       
        query = "SELECT COUNT(*) AS row_count FROM [dbo].[ThreatsDetectedTable]"
        row_count = execute_query(query)[0]["row_count"]
       
        return {
            "database": os.getenv("FABRIC_DATABASE"),
            "collation": collation,
            "threats_detected_row_count": row_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.post("/api/fabric/debug")
def debug_query(question: str):
    """Debug SQL generation"""
    try:
        tables_info = get_tables_info()
        prompt = build_smart_prompt(question, tables_info)
        raw_response = ask_llm(prompt)
        cleaned_sql = clean_generated_sql(raw_response)
       
        return {
            "question": question,
            "raw_llm_response": raw_response,
            "cleaned_sql": cleaned_sql,
            "starts_with_select": cleaned_sql.upper().startswith("SELECT"),
            "tables_available": len(list_fabric_tables()),
            "available_table_names": [t["full_name"] for t in list_fabric_tables()],
            "tables_info_preview": tables_info[:500] + "..." if len(tables_info) > 500 else tables_info
        }
    except Exception as e:
        return {"error": str(e)}
 
if __name__ == "__main__":
    print("Starting Microsoft Fabric SQL Analytics MCP Server...")
    print("Key features:")
    print("- Standard T-SQL for Fabric SQL Database")
    print("- Schema-qualified table names (e.g., [dbo].[Table Name])")
    print("- Debug endpoints for table access and query generation")
    print("- Case-sensitive handling for Severity values")
    print("")
    print("Endpoints:")
    print("- POST /api/fabric/smart-analyze - Smart query analysis")
    print("- GET  /api/fabric/health - Health check")
    print("- GET  /api/fabric/list-tables - List all tables")
    print("- POST /api/fabric/direct-test - Test table access")
    print("- GET  /api/fabric/inspect-table/{table_name} - Inspect table data")
    print("- GET  /api/fabric/database-info - Database information")
    print("- POST /api/fabric/debug - Debug query generation")
    print("")
    uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True)
