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

# Global variable for database collation
DATABASE_COLLATION = None

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

def get_database_collation():
    """Get database collation"""
    global DATABASE_COLLATION
    if DATABASE_COLLATION is None:
        try:
            query = "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS Collation"
            result = execute_query(query)
            DATABASE_COLLATION = result[0]["Collation"] if result else "Unknown"
            print(f"Database collation: {DATABASE_COLLATION}")  # Debug
        except Exception as e:
            print(f"Failed to get collation: {str(e)}")
            DATABASE_COLLATION = "Unknown"
    return DATABASE_COLLATION

def is_collation_case_insensitive():
    """Check if database collation is case-insensitive"""
    collation = get_database_collation()
    return "CI" in collation.upper() if collation != "Unknown" else False

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
                {"role": "system", "content": "You are a Microsoft Fabric SQL Database expert. Always start queries with SELECT and use [schema].[Table Name] for tables. Use case-insensitive comparisons for string values and column names."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,  # Slightly higher to encourage attempts
            max_tokens=1000  # Increased for complex queries
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
    """Get all tables with their columns, sample data, and unique values"""
    all_tables = list_fabric_tables()
    tables_info = []
    
    for table in all_tables:
        try:
            schema_info = get_table_schema(table["full_name"])
            columns = [f"[{col['COLUMN_NAME']}] ({col['DATA_TYPE']}, Nullable: {col['IS_NULLABLE']})" for col in schema_info['columns']]
            
            # Get sample data
            sample_data = []
            try:
                sample_query = f"SELECT TOP 5 * FROM [{table['schema']}].[{table['name']}]"
                sample_data = execute_query(sample_query)
            except Exception as e:
                print(f"Failed to get sample data for {table['full_name']}: {str(e)}")
            
            # Get unique values for string columns
            unique_values = {}
            for col in schema_info['columns']:
                if col['DATA_TYPE'] in ('varchar', 'nvarchar', 'char', 'nchar'):
                    try:
                        unique_query = f"SELECT DISTINCT [{col['COLUMN_NAME']}] FROM [{table['schema']}].[{table['name']}]] WHERE [{col['COLUMN_NAME']}] IS NOT NULL"
                        results = execute_query(unique_query)
                        unique_values[col['COLUMN_NAME']] = [row[col['COLUMN_NAME']] for row in results][:10]
                    except Exception as e:
                        unique_values[col['COLUMN_NAME']] = f"Error: {str(e)}"
            
            table_info = (
                f"Table: [{table['schema']}].[{table['name']}]\n"
                f"Columns: {', '.join(columns)}\n"
                f"Sample Data: {json.dumps(sample_data, indent=2, default=str)}\n"
                f"Unique Values (string columns): {json.dumps(unique_values, indent=2, default=str)}"
            )
            tables_info.append(table_info)
        except Exception as e:
            print(f"Skipping schema for {table['full_name']}: {str(e)}")
            tables_info.append(f"Table: [{table['schema']}].[{table['name']}] (Schema unavailable)")
    
    return "\n\n".join(tables_info)

def build_smart_prompt(question: str, tables_info: str, limit: int) -> str:
    """Build prompt for smart SQL generation"""
    case_insensitive = is_collation_case_insensitive()
    case_instruction = (
        "For string columns (varchar, nvarchar, char, nchar), use LOWER([Column]) = LOWER('value') for case-insensitive comparisons. "
        "For numeric or boolean columns (int, bit, float), use direct comparisons (e.g., [Column] = 0 or [Column] = 1)."
        if not case_insensitive else
        "String comparisons are case-insensitive due to database collation. For numeric or boolean columns, use direct comparisons."
    )
    
    return f"""
Generate a Microsoft Fabric SQL Database query to answer this question.

CRITICAL RULES for Fabric SQL Database:
1. ALWAYS start with "SELECT"
2. ALWAYS use [schema].[Table Name] format for tables (e.g., [dbo].[TableName])
3. Use exact column names as listed, with brackets for names with spaces (e.g., [Column Name])
4. Include TOP {limit} for performance
5. Return only the SQL query, no explanations
6. If uncertain, attempt a query based on the most relevant table and columns, using sample values to guide conditions
7. Do NOT omit schema prefix (e.g., use [dbo].[Table Name], not [Table Name])
8. {case_instruction}
9. Support aggregation functions (e.g., COUNT, SUM, AVG) for questions asking for counts or summaries
10. Table and column names are case-insensitive; normalize to match schema
11. For listing queries, select identifier columns (e.g., [DeviceID], [Device Name]) and relevant attributes, avoiding non-device identifiers like [Employee ID]
12. For 'outdated' conditions, prioritize boolean/numeric columns (e.g., [Status] = 0) or date columns (e.g., [LastUpdateDate] < GETDATE() - 30) based on sample values

Available Tables:
{tables_info}

Question: {question}

Examples:
- SELECT TOP {limit} [DeviceID], [Device Name], [OS Type], [Endpoint_protection] FROM [dbo].[DeviceSecurityTable] WHERE [Endpoint_protection] = 0
- SELECT TOP {limit} [DeviceID], [Device Name], [Antivirus Version] FROM [dbo].[DeviceSecurityTable] WHERE [LastUpdateDate] < DATEADD(day, -30, GETDATE())
- SELECT TOP {limit} COUNT(*) AS ThreatCount, [Threat Type] FROM [dbo].[ThreatsDetectedTable] WHERE LOWER([Severity]) = LOWER('high') GROUP BY [Threat Type]
- SELECT TOP {limit} [t1].[Employee ID], [t2].[Department] FROM [dbo].[Employees] [t1] JOIN [dbo].[Departments] [t2] ON [t1].[Department ID] = [t2].[Department ID]

Return only the complete SQL query starting with SELECT:
"""
# --- Pydantic Models ---

class SmartQueryRequest(BaseModel):
    question: str
    limit: int = 100

class DirectTestRequest(BaseModel):
    table_name: str

# --- MCP Tools ---

@mcp.tool("fabric.smart_analyze")
def smart_analyze_question(question: str, limit: int = 100) -> Dict[str, Any]:
    """Smart analysis with table name validation"""
    import re
    try:
        available_tables = list_fabric_tables()
        if not available_tables:
            return {
                "question": question,
                "error": "No tables found in Fabric SQL Database",
                "suggestion": "Check your connection and table permissions"
            }
        
        tables_info = get_tables_info()
        prompt = build_smart_prompt(question, tables_info, limit)
        generated_sql_raw = ask_llm(prompt)
        generated_sql = clean_generated_sql(generated_sql_raw)
        
        print("Generated SQL:", generated_sql)  # Debug
        
        if generated_sql.strip().upper() == "INSUFFICIENT_DATA":
            print("LLM returned INSUFFICIENT_DATA; attempting best-guess query")  # Debug
            relaxed_prompt = prompt.replace("If uncertain, attempt a query based on the most relevant table and columns", "Make a best-guess query using the most relevant table and columns, even if uncertain")
            generated_sql_raw = ask_llm(relaxed_prompt)
            generated_sql = clean_generated_sql(generated_sql_raw)
            print("Relaxed SQL:", generated_sql)  # Debug
        
        if not generated_sql.upper().startswith("SELECT"):
            return {
                "question": question,
                "error": "Generated query is not a SELECT statement",
                "generated_sql": generated_sql
            }
        
        if "TOP" not in generated_sql.upper():
            generated_sql = generated_sql.replace("SELECT", f"SELECT TOP {limit}", 1)
        
        all_valid_names = [f"[{t['schema']}].[{t['name']}]".lower() for t in available_tables]
        query_lower = generated_sql.lower()
        has_valid_table = any(name in query_lower for name in all_valid_names)
        
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
            suggestions = []
            table_name = next((name for name in all_valid_names if name in query_lower), None)
            if table_name:
                schema, table = table_name.strip('[]').split('.')
                try:
                    schema_info = get_table_schema(f"{schema}.{table}")
                    for col in schema_info['columns']:
                        if col['DATA_TYPE'] in ('bit', 'int'):
                            suggestions.append(f"Try [{col['COLUMN_NAME']}] = 0 or [{col['COLUMN_NAME']}] = 1")
                        elif col['DATA_TYPE'] in ('datetime', 'date'):
                            suggestions.append(f"Try [{col['COLUMN_NAME']}] < DATEADD(day, -30, GETDATE())")
                        elif col['DATA_TYPE'] in ('varchar', 'nvarchar', 'char', 'nchar'):
                            suggestions.append(f"Try LOWER([{col['COLUMN_NAME']}]) = LOWER('outdated')")
                except Exception as e:
                    suggestions.append(f"Error inspecting table: {str(e)}")
            
            return {
                "question": question,
                "generated_sql": generated_sql,
                "analysis": "Query executed but returned no results",
                "available_tables": [t["full_name"] for t in available_tables],
                "suggestions": suggestions or ["Check column values using /api/fabric/inspect-table", "Try alternative conditions (e.g., date-based or different status values)"]
            }
        
        context = json.dumps(results[:limit], indent=2, default=str)  # Use limit for context
        # Detect listing queries more broadly
        is_listing_query = bool(re.match(r'^\s*(list|show|display|which|what)\b', question.lower(), re.IGNORECASE))
        
        if is_listing_query:
            analysis_prompt = f"""
Based on this data, provide a formatted list of the results to answer the question. Include all fields for each item, using clear labels.

Data: {context}
Question: {question}

Format the response as a numbered list, e.g.:
1. Device: [Field1: Value1, Field2: Value2, ...]
2. Device: [Field1: Value1, Field2: Value2, ...]
If no results, state: "No items found matching the criteria."
Ensure all returned items are listed, up to the provided data limit.
"""
        else:
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
            "result_count": len(results),
            "results": results
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
            "connection": "ok",
            "collation": get_database_collation()
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
def direct_fabric_test(req: DirectTestRequest):
    """Test direct access to a specified table"""
    table_name = req.table_name
    test_queries = [
        f"SELECT TOP 1 * FROM [{table_name}]",  # Sample data
        f"SELECT COUNT(*) AS row_count FROM [{table_name}]"  # Check total rows
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
                "data": result[:5]
            }
        except Exception as e:
            results[f"query_{i+1}"] = {
                "query": query,
                "status": "FAILED",
                "error": str(e)[:200]
            }
    
    return {
        "message": f"Testing access to '{table_name}'",
        "results": results,
        "recommendation": "Inspect data in successful queries to verify table access"
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
        stats = {}
        for col in columns:
            try:
                unique_query = f"SELECT DISTINCT [{col}] FROM [{schema_info['table_name']}] WHERE [{col}] IS NOT NULL"
                results = execute_query(unique_query)
                unique_values[col] = [row[col] for row in results][:10]
                
                count_query = f"SELECT COUNT(DISTINCT [{col}]) AS unique_count, COUNT(*) AS total_count FROM [{schema_info['table_name']}]"
                count_result = execute_query(count_query)
                stats[col] = {
                    "unique_count": count_result[0]["unique_count"],
                    "total_count": count_result[0]["total_count"]
                }
            except Exception as e:
                unique_values[col] = f"Error: {str(e)}"
                stats[col] = f"Error: {str(e)}"
        
        return {
            "table": schema_info["table_name"],
            "columns": columns,
            "sample_data": sample_data,
            "unique_values": unique_values,
            "column_stats": stats
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
        
        return {
            "database": os.getenv("FABRIC_DATABASE"),
            "collation": collation
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/fabric/debug")
def debug_query(question: str):
    """Debug SQL generation"""
    try:
        tables_info = get_tables_info()
        limit = 100
        prompt = build_smart_prompt(question, tables_info, limit)
        raw_response = ask_llm(prompt)
        cleaned_sql = clean_generated_sql(raw_response)
        
        debug_info = {
            "question": question,
            "raw_llm_response": raw_response,
            "cleaned_sql": cleaned_sql,
            "starts_with_select": cleaned_sql.upper().startswith("SELECT"),
            "tables_available": len(list_fabric_tables()),
            "available_table_names": [t["full_name"] for t in list_fabric_tables()],
            "tables_info_preview": tables_info[:1000] + "..." if len(tables_info) > 1000 else tables_info
        }
        
        if cleaned_sql.strip().upper() == "INSUFFICIENT_DATA":
            debug_info["insufficient_data_reason"] = (
                "LLM determined no suitable table/columns match the question. "
                "Check table schemas and column names in 'tables_info_preview'."
            )
        
        return debug_info
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    print("Starting Microsoft Fabric SQL Analytics MCP Server...")
    print("Key features:")
    print("- Standard T-SQL for Fabric SQL Database")
    print("- Case-insensitive table/column names and string comparisons")
    print("- Configurable result limits")
    print("- Support for aggregation and listing queries")
    print("- Generalized for any dataset")
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
    uvicorn.run("__main__:app", host="0.0.0.0", port=8001, reload=True)
