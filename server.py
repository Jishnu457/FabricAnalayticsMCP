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
from typing import List, Dict, Any, Optional
import json
from datetime import datetime, date
from decimal import Decimal
import statistics
import math
from functools import lru_cache

@lru_cache(maxsize=100)
def cached_intelligent_analyze(question: str) -> Dict[str, Any]:
    return intelligent_analyze(question)

 
def safe_json_serialize(obj):
    """Custom JSON serializer for datetime and other objects"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif hasattr(obj, '__dict__'):
        return str(obj)
    return obj
 
# Load environment variables
load_dotenv()
 
# FastAPI app
app = FastAPI(title="Intelligent Microsoft Fabric SQL Analytics")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
 
# MCP server
mcp = FastMCP("Intelligent Fabric Analytics", dependencies=["pyodbc", "fastapi", "python-dotenv", "pandas"])
 
# Azure OpenAI client
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)
 
def get_fabric_connection():
    """Create connection to Microsoft Fabric SQL Database"""
    server = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_DATABASE")
    client_id = os.getenv("FABRIC_CLIENT_ID")
    client_secret = os.getenv("FABRIC_CLIENT_SECRET")
    tenant_id = os.getenv("FABRIC_TENANT_ID")
   
    if not all([server, database, client_id, client_secret, tenant_id]):
        raise Exception("Missing required environment variables")
   
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
        return conn
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        raise
 
def execute_query(query: str, params=None) -> List[Dict[str, Any]]:
    """Execute query and return results with proper datetime handling"""
    conn = get_fabric_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
       
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
       
        # Convert results with proper datetime handling
        result = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                # Handle different data types for JSON serialization
                if isinstance(value, datetime):
                    row_dict[columns[i]] = value.isoformat()
                elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                    row_dict[columns[i]] = value.isoformat()
                elif isinstance(value, (bytes, bytearray)):
                    row_dict[columns[i]] = value.decode('utf-8', errors='ignore')
                elif value is None:
                    row_dict[columns[i]] = None
                else:
                    row_dict[columns[i]] = value
            result.append(row_dict)
       
        return result
    except Exception as e:
        print(f"Query execution error: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
 
def list_fabric_tables():
    """List tables in Fabric SQL Database"""
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
   
    results = execute_query(query)
    tables = []
    for row in results:
        tables.append({
            "schema": row["TABLE_SCHEMA"],
            "name": row["TABLE_NAME"],
            "full_name": f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
        })
   
    return tables
 
def get_tables_info():
    all_tables = list_fabric_tables()
    tables_info = []
    for table in all_tables:
        column_query = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        columns = execute_query(column_query, (table["schema"], table["name"]))
        fk_query = """
        SELECT
            C.CONSTRAINT_NAME,
            C.TABLE_NAME,
            C.COLUMN_NAME,
            R.TABLE_NAME AS REFERENCED_TABLE,
            R.COLUMN_NAME AS REFERENCED_COLUMN
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
            ON C.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE R
            ON R.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
        WHERE C.TABLE_SCHEMA = ? AND C.TABLE_NAME = ?
        """
        fks = execute_query(fk_query, (table["schema"], table["name"]))
        fk_info = [
            f"{fk['COLUMN_NAME']} references {fk['REFERENCED_TABLE']}.{fk['REFERENCED_COLUMN']}"
            for fk in fks
        ]
        try:
            sample_query = f"SELECT TOP 3 * FROM [{table['schema']}].[{table['name']}]"
            sample_data = execute_query(sample_query)
        except:
            sample_data = []
        enhanced_columns = []
        numeric_columns = []
        text_columns = []
        date_columns = []
        column_values = {}  # Add distinct values for varchar columns
        for col in columns:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE'].lower()
            nullable = 'Nullable' if col['IS_NULLABLE'] == 'YES' else 'Not Nullable'
            if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
                numeric_columns.append(col_name)
                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - NUMERIC: Use AVG(), SUM(), MAX(), MIN()")
            elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                text_columns.append(col_name)
                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - TEXT: Use COUNT(), CASE statements, GROUP BY - NEVER AVG()")
                # Fetch distinct values for varchar columns (limit to 10 for performance)
                try:
                    distinct_query = f"SELECT DISTINCT TOP 10 [{col_name}] FROM [{table['schema']}].[{table['name']}] WHERE [{col_name}] IS NOT NULL"
                    distinct_values = execute_query(distinct_query)
                    column_values[col_name] = [row[col_name] for row in distinct_values]
                except:
                    column_values[col_name] = []
            elif data_type in ['datetime', 'datetime2', 'date', 'time', 'datetimeoffset', 'smalldatetime']:
                date_columns.append(col_name)
                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - DATE: Use MAX(), MIN(), date functions")
            else:
                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable})")
        table_info = {
            "table": f"[{table['schema']}].[{table['name']}]",
            "columns": enhanced_columns,
            "numeric_columns": numeric_columns,
            "text_columns": text_columns,
            "date_columns": date_columns,
            "foreign_keys": fk_info,
            "sample_data": sample_data[:2] if sample_data else [],
            "column_values": column_values  # Add distinct values to schema
        }
        tables_info.append(table_info)
    return tables_info
 
def clean_generated_sql(sql_text: str) -> str:
    """Clean SQL from LLM response - now the LLM should understand data types itself"""
    if not sql_text:
        return ""
   
    # Remove markdown and clean the text
    sql = sql_text.strip()
   
    # Remove code block markers
    if sql.startswith('```'):
        lines = sql.split('\n')
        # Find the actual SQL content between code blocks
        start_idx = 1 if lines[0].startswith('```') else 0
        end_idx = len(lines)
        for i, line in enumerate(lines[1:], 1):
            if line.strip().startswith('```'):
                end_idx = i
                break
        sql = '\n'.join(lines[start_idx:end_idx])
   
    # Take only the first complete SELECT statement
    lines = sql.split('\n')
    sql_lines = []
    in_select = False
   
    for line in lines:
        line = line.strip()
        if not line or line.startswith('--'):
            continue
           
        # Start capturing when we see SELECT
        if line.upper().startswith('SELECT'):
            in_select = True
            sql_lines = [line]  # Reset and start fresh
        elif in_select:
            # Continue capturing SQL lines
            if any(keyword in line.upper() for keyword in
                  ['FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'GROUP', 'HAVING', 'ORDER', 'AND', 'OR']):
                sql_lines.append(line)
            elif line.upper().startswith('SELECT'):
                # Hit another SELECT - stop here (we only want the first complete query)
                break
            elif line.endswith(',') or line.endswith('(') or '(' in line:
                # Continuation line
                sql_lines.append(line)
            else:
                # Probably end of SQL
                break
   
    # Join and clean
    sql = ' '.join(sql_lines)
    sql = sql.strip().rstrip(';').rstrip(',')
   
    # Validate basic structure
    if sql:
        sql_upper = sql.upper()
        if not sql_upper.startswith('SELECT'):
            return ""
        if 'FROM' not in sql_upper:
            return ""
        # Check for obviously malformed patterns
        if 'FROM FROM' in sql_upper or ', FROM' in sql_upper:
            return ""
   
    return sql
 
def should_generate_visualization(question: str, sql: str, results: List[Dict[str, Any]]) -> bool:
    """Determine if a visualization should be generated based on the question, SQL, and results"""
    if not results or len(results) < 1:
        return False

    # Only generate visualizations if explicitly requested in the question
    visualization_keywords = ["chart", "graph", "visualize", "plot", "display", "trend", "distribution","compare", "percentage", "over time"]
    if not any(keyword in question.lower() for keyword in visualization_keywords):
        return False

    # Analyze the results for suitability
    columns = list(results[0].keys())
    numeric_cols = [col for col in columns if any(isinstance(row[col], (int, float)) for row in results)]
    text_cols = [col for col in columns if any(isinstance(row[col], str) for row in results)]
    date_cols = [col for col in columns if any(isinstance(row[col], str) and "-" in row[col] for row in results)]  # Simple date check

    # Suitable for visualization if:
    # - Has at least one numeric column and one text/date column (for labels)
    # - Results are not too large (e.g., <= 50 rows for clarity)
    suitable_data = (
        len(numeric_cols) >= 1 and
        (len(text_cols) >= 1 or len(date_cols) >= 1) and
        len(results) <= 50
    )

    return suitable_data
 
def generate_visualization(results: List[Dict[str, Any]], question: str) -> Optional[Dict]:
    """Generate a Chart.js configuration for visualization based on query results"""
    if not results or len(results) < 1:
        return None
 
    # Analyze the data to determine the best chart type
    columns = list(results[0].keys())
    numeric_cols = [col for col in columns if any(isinstance(row[col], (int, float)) for row in results)]
    text_cols = [col for col in columns if any(isinstance(row[col], str) for row in results)]
    date_cols = [col for col in columns if any(isinstance(row[col], str) and "-" in row[col] for row in results)]  # Simple date check
 
    # Only generate visualization for suitable data (e.g., categorical + numeric data)
    if len(numeric_cols) < 1 or (len(text_cols) < 1 and len(date_cols) < 1):
        return None
 
    # Choose the first text/date column as labels and the first numeric column as values
    label_col = date_cols[0] if date_cols else text_cols[0]
    value_col = numeric_cols[0]
 
    # Limit to top 10 results to avoid overwhelming the chart
    data = results[:10]
    labels = [row[label_col] for row in data]
    values = [row[value_col] for row in data]
 
    # Determine chart type based on question context and data
    chart_type = "bar"
    if "trend" in question.lower() or "over time" in question.lower() or date_cols:
        chart_type = "line"
    elif "distribution" in question.lower() or "percentage" in question.lower():
        chart_type = "pie"
 
    # Generate Chart.js configuration (updated for v4 syntax)
    chart_config = {
        "type": chart_type,
        "data": {
            "labels": labels,
            "datasets": [{
                "label": value_col,
                "data": values,
                "backgroundColor": [
                    "rgba(75, 192, 192, 0.7)",
                    "rgba(255, 99, 132, 0.7)",
                    "rgba(54, 162, 235, 0.7)",
                    "rgba(255, 206, 86, 0.7)",
                    "rgba(153, 102, 255, 0.7)",
                    "rgba(255, 159, 64, 0.7)",
                    "rgba(199, 199, 199, 0.7)",
                    "rgba(83, 102, 255, 0.7)",
                    "rgba(255, 99, 71, 0.7)",
                    "rgba(50, 205, 50, 0.7)"
                ],
                "borderColor": [
                    "rgba(75, 192, 192, 1)",
                    "rgba(255, 99, 132, 1)",
                    "rgba(54, 162, 235, 1)",
                    "rgba(255, 206, 86, 1)",
                    "rgba(153, 102, 255, 1)",
                    "rgba(255, 159, 64, 1)",
                    "rgba(199, 199, 199, 1)",
                    "rgba(83, 102, 255, 1)",
                    "rgba(255, 99, 71, 1)",
                    "rgba(50, 205, 50, 1)"
                ],
                "borderWidth": 1
            }]
        },
        "options": {
            "scales": {
                "y": {
                    "beginAtZero": True,
                    "type": "linear"  # Explicitly specify scale type for v4
                },
                "x": {
                    "type": "category"  # Explicitly specify scale type for v4
                }
            } if chart_type != "pie" else {},
            "plugins": {
                "legend": {
                    "display": chart_type == "pie"
                },
                "title": {
                    "display": True,
                    "text": f"{value_col} by {label_col}"
                }
            }
        }
    }
 
    return chart_config
 
def build_chatgpt_system_prompt(question: str, tables_info: List[Dict], conversation_history: List[Dict] = None) -> str:
    """Build a ChatGPT-like system prompt for natural conversation and calculations"""
   
    context_analysis = ""
    if conversation_history:
        recent_topics = " ".join([msg.get("content", "") for msg in conversation_history[-3:]])
        context_analysis = f"\n\nCONVERSATION CONTEXT: {recent_topics}"
   
    return f"""You are an expert SQL analyst. You must respond in this EXACT format:
 
SQL_QUERY:
[Single, complete, valid SQL statement using fully qualified names like [schema].[table].[column] and table aliases]
 
ANALYSIS:
[Explain metrics, assess effectiveness, and provide actionable recommendations]
 
CRITICAL SQL RULES:
1. Generate ONLY ONE complete SQL statement - no CTEs, no multiple queries
2. Use simple JOINs with table aliases
3. Start with SELECT, include FROM with table name, proper JOINs with ON conditions
4. Never generate partial queries or multiple SELECT statements
5. Use exact table and column names from schema: [schema].[table].[column]
6. Keep queries simple but effective
7. Only use AVG(), SUM() on numeric columns (check schema for data types:int, float, decimal etc.)
8. For bit columns (e.g., EndpointProtection), use SUM(CASE WHEN [column] = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 for percentages
    - If the question asks for raw evaluation (e.g., list True/False values), SELECT the column directly with a WHERE clause (e.g., WHERE [column] = 1 for True)
   - If the question asks for effectiveness or proportions, use SUM(CASE WHEN [column] = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 for percentages
9. For varchar columns, use COUNT() or GROUP BY, not AVG()
10. For date columns, use DATEPART() for time-based grouping (e.g., DATEPART(MONTH, [column]))
11. For questions about trends, include time-based grouping if a date column exists
12. For questions about effectiveness or proportions, calculate percentages where appropriate
13. Group by relevant categorical columns (e.g., DeviceType,etc)
14. Order results by a meaningful metric (e.g., COUNT, SUM, percentage, or a key column)
15. For WHERE clauses involving varchar columns, use values from the 'column_values' in the schema
    - If a user-provided value (e.g., 'resolved') doesn't match the column_values, map it to the closest valid value or exclude the condition
    - Example: For Status, if 'resolved' is used and column_values lists ['Closed', 'In Progress', 'Open'], map 'resolved' to 'Closed'
 
SIMPLE PATTERN APPROACH:
For pattern analysis, use straightforward JOINs and aggregation:
- Join UserActivity with Devices to get device names
- Filter for failed logins in recent timeframe
- Count failed logins per device
- Order by count to find unusual patterns
 
EXAMPLE VALID SQL (what TO write):
```sql
SELECT TOP 50
    d.[DeviceID],
    d.[Name] as DeviceName,
    COUNT(ua.[ActivityID]) as FailedLoginCount,
    MAX(ua.[ActivityDate]) as LastFailedLogin
FROM [dbo].[UserActivity] ua
INNER JOIN [dbo].[Devices] d ON ua.[DeviceID] = d.[DeviceID]
WHERE ua.[Status] = 'Failed'
    AND ua.[ActivityDate] >= DATEADD(day, -30, GETDATE())
GROUP BY d.[DeviceID], d.[Name]
HAVING COUNT(ua.[ActivityID]) > 5
ORDER BY FailedLoginCount DESC
```
 
WHAT NOT TO WRITE:
❌ Multiple SELECT statements
❌ CTEs with WITH clauses
❌ Incomplete FROM clauses
❌ Missing table names in FROM
❌ Complex nested queries
 
AVAILABLE SCHEMA:
{json.dumps(tables_info, indent=2, default=safe_json_serialize)}
 
{context_analysis}
 
USER QUESTION: "{question}"
 
Generate ONE simple, complete SQL query using aliases and fully qualified names:"""
 
def ask_intelligent_llm(prompt: str) -> str:
    """Send prompt to Azure OpenAI with ChatGPT-like parameters"""
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not deployment:
        raise ValueError("AZURE_OPENAI_DEPLOYMENT not set")
   
    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful, friendly AI assistant similar to ChatGPT, with expertise in data analysis and business intelligence. Be conversational, explain calculations clearly, and provide actionable insights."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=2000,
            seed = 42
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"LLM request failed: {str(e)}")
        raise
 
# Updated request model to remove visualize flag
class IntelligentRequest(BaseModel):
    question: str
 
@mcp.tool("fabric.intelligent_analyze")
def intelligent_analyze(question: str) -> Dict[str, Any]:
    """Enhanced intelligent analysis with ChatGPT-like behavior, calculations, and smart visualization"""
    try:
        tables_info = get_tables_info()
        if not tables_info:
            return {
                "question": question,
                "error": "No accessible tables found",
                "suggestion": "Check database connection and permissions"
            }
       
        # Use LLM for all questions - let it be intelligent
        prompt = build_chatgpt_system_prompt(question, tables_info)
        llm_response = ask_intelligent_llm(prompt)
       
        # Extract SQL from response using the structured format
        if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response:
            parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
            potential_sql = clean_generated_sql(parts[0].strip())
            analysis = parts[1].strip() if len(parts) > 1 else "Analysis not provided"
        else:
            # Fallback to old method if format not followed
            parts = llm_response.split('\n\n', 1)
            if len(parts) >= 2:
                potential_sql = clean_generated_sql(parts[0])
                analysis = parts[1] if len(parts) > 1 else llm_response
            else:
                # Last resort: try to extract any SQL
                lines = llm_response.split('\n')
                sql_lines = []
                analysis_lines = []
                found_sql = False
               
                for line in lines:
                    if line.strip().upper().startswith('SELECT'):
                        found_sql = True
                        sql_lines.append(line.strip())
                    elif found_sql and any(keyword in line.upper() for keyword in
                                         ['FROM', 'WHERE', 'ORDER', 'GROUP', 'JOIN', 'LEFT', 'INNER', 'ON']):
                        sql_lines.append(line.strip())
                    elif found_sql and line.strip() == '':
                        break
                    elif not found_sql:
                        continue
                    else:
                        analysis_lines.append(line)
               
                potential_sql = clean_generated_sql(' '.join(sql_lines))
                analysis = '\n'.join(analysis_lines) if analysis_lines else llm_response
       
        if not potential_sql or not potential_sql.upper().startswith("SELECT"):
            # For data analysis questions, try harder to generate SQL
            if any(keyword in question.lower() for keyword in ["show", "which", "what", "find", "analyze", "unpatched", "devices", "vulnerabilities"]):
                force_sql_prompt = f"""This is a data analysis question that requires SQL generation.
 
Question: {question}
 
You MUST generate SQL, not a conversational response. Use the available schema to create a query that answers this specific question.
 
Available tables and columns:
{json.dumps(tables_info[:4], indent=2, default=safe_json_serialize)}
 
Known values:
- PatchStatus: 'Unpatched', 'Patched', 'Mitigated'
- Use [DeviceName] for device names
 
Generate a comprehensive SQL query with proper JOINs to answer the question.
 
Format:
SQL_QUERY:
[Complete SQL with JOINs and WHERE clauses]
 
ANALYSIS:
[Brief explanation]"""
 
                try:
                    forced_response = ask_intelligent_llm(force_sql_prompt)
                    if "SQL_QUERY:" in forced_response:
                        forced_parts = forced_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                        forced_sql = clean_generated_sql(forced_parts[0].strip())
                       
                        if forced_sql and forced_sql.upper().startswith("SELECT") and "FROM" in forced_sql.upper():
                            potential_sql = forced_sql
                            analysis = forced_parts[1].strip() if len(forced_parts) > 1 else "Analysis provided"
                        else:
                            raise Exception("Could not generate valid SQL")
                    else:
                        raise Exception("No SQL generated")
                       
                except Exception:
                    return {
                        "question": question,
                        "error": "Could not generate SQL for this data analysis question",
                        "analysis": "This appears to be a complex data analysis question that should generate SQL. There might be an issue with the query complexity or available data structure.",
                        "suggestion": "Try asking about specific aspects: 'Show me unpatched vulnerabilities' or 'Show me non-compliant devices'"
                    }
            else:
                # Handle non-SQL questions conversationally
                conversational_prompt = f"""The user asked: "{question}"
 
This doesn't seem to require database analysis. Provide a helpful, ChatGPT-like response that:
1. Addresses their question directly
2. Explains relevant concepts if needed
3. Offers to help with data analysis if they have specific questions
4. Suggests how they might explore their data
 
Be friendly and conversational, like ChatGPT would respond."""
               
                conversational_response = ask_intelligent_llm(conversational_prompt)
                return {
                    "question": question,
                    "response_type": "conversational",
                    "analysis": conversational_response,
                    "timestamp": datetime.now().isoformat()
                }
       
        # Execute the query
        try:
            results = execute_query(potential_sql)
        except Exception as e:
            # If SQL fails, try to generate a corrected version
            error_msg = str(e)
           
            # For this specific type of complex question, provide a simpler alternative
            if any(keyword in question.lower() for keyword in ["unusual activity", "pattern", "insider threat", "after-hours", "behavior"]):
                fallback_prompt = f"""Generate a simple SQL query for behavioral analysis.
 
Question: {question}
 
Create a simple query that analyzes user activity patterns:
1. Focus on recent activity (last 30 days)
2. Use simple DATEPART() for time analysis (no datetime-to-float conversions)
3. Use basic JOINs only
4. Look for measurable patterns (activity counts, timing)
 
Available tables: UserActivity (UserID, ActivityDate, ActivityType, Status), EmployeeInfo (EmployeeID, Name, Department)
 
Use DATEPART(hour, ActivityDate) for time analysis.
For after-hours: DATEPART(hour, ActivityDate) >= 18 OR DATEPART(hour, ActivityDate) <= 6
 
Format:
SQL_QUERY:
[Simple SELECT with proper datetime handling]
 
ANALYSIS:
[Brief explanation]"""
 
                try:
                    simple_response = ask_intelligent_llm(fallback_prompt)
                    if "SQL_QUERY:" in simple_response:
                        simple_parts = simple_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                        simple_sql = clean_generated_sql(simple_parts[0].strip())
                       
                        if simple_sql and simple_sql.upper().startswith("SELECT") and "FROM" in simple_sql.upper():
                            results = execute_query(simple_sql)
                            potential_sql = simple_sql
                        else:
                            raise Exception("Could not generate valid simple SQL")
                    else:
                        raise Exception("No simple SQL provided")
                       
                except Exception:
                    return {
                        "question": question,
                        "error": f"Complex query failed: {error_msg}",
                        "generated_sql": potential_sql,
                        "analysis": "This question requires complex behavioral pattern analysis. Let me suggest breaking this down into simpler questions:\n\n• 'Show me users with after-hours activity in the last 30 days'\n• 'Which users access sensitive data most frequently?'\n• 'Show me users with unusual activity timing patterns'\n\nThese simpler queries will help identify behavioral patterns step by step.",
                        "suggestion": "Try: 'Show me after-hours user activity' or 'Which users access sensitive data?'"
                    }
            else:
                return {
                    "question": question,
                    "error": f"Query execution failed: {error_msg}",
                    "generated_sql": potential_sql,
                    "analysis": f"I encountered a syntax issue with the SQL query. Let me help you with a different approach.\n\nCould you try asking a more specific question about what aspect of the data you'd like to analyze?",
                    "suggestion": "Try asking about specific metrics or data points you're interested in"
                }
       
        # Determine if visualization is appropriate
        visualization = None
        if should_generate_visualization(question, potential_sql, results):
            visualization = generate_visualization(results, question)
 
        # Generate intelligent analysis
        if results:
            enhanced_prompt = f"""User Question: {question}
 
Query Results: {len(results)} records found
Sample Data: {json.dumps(results[:10], indent=2, default=safe_json_serialize)}
 
Provide a natural, ChatGPT-like response that starts with a brief introduction summarizing the results (e.g., the actual data like specific values or records) without labeling it as a separate section (e.g., avoid using a 'Direct Answer' heading). Then include the following sections:

-Business Context
Explain what the data means in a business context.

- Key Patterns and Insights
Identify key patterns, trends, or insights, incorporating relevant data points as needed to support the analysis.

- Actionable Recommendations
Provide actionable recommendations based on the data.

- Follow-Up Analysis
Suggest relevant follow-up analysis.
 
Be conversational and focus on business impact. Calculate any additional percentages or ratios that would be helpful for understanding the data."""
           
            enhanced_analysis = ask_intelligent_llm(enhanced_prompt)
        else:
            no_data_prompt = f"""The query for "{question}" returned no results.
 
Query: {potential_sql}
 
Provide a helpful explanation of:
1. Why no data was found (empty table, filters too restrictive, etc.)
2. What this means in business context
3. Suggestions for alternative analysis approaches
4. Next steps they could take
 
Be conversational and helpful."""
           
            enhanced_analysis = ask_intelligent_llm(no_data_prompt)
       
        response = {
            "question": question,
            "generated_sql": potential_sql,
            "analysis": enhanced_analysis,
            "result_count": len(results),
            "sample_data": results[:5] if results else [],
            "timestamp": datetime.now().isoformat()
        }
       
        if visualization:
            response["visualization"] = visualization
       
        return response
       
    except Exception as e:
        return {
            "question": question,
            "error": f"Analysis failed: {str(e)}",
            "analysis": "I encountered an error while analyzing your question. Could you try rephrasing it or being more specific about what you'd like to know?",
            "suggestion": "Try asking about specific metrics like 'What is the average cyber risk score?' or 'How many high-risk devices do we have?'"
        }
 
# Updated FastAPI endpoint to remove visualize flag
@app.post("/api/fabric/intelligent-analyze")
def intelligent_analyze_endpoint(req: IntelligentRequest):
    """ChatGPT-like intelligent endpoint with calculations and smart visualization"""
    try:
        result = cached_intelligent_analyze(question=req.question)  # Use cached version
        if "error" in result and result.get("response_type") != "conversational":
            raise HTTPException(status_code=400, detail=result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/fabric/health")
def health_check():
    """Health check endpoint"""
    try:
        execute_query("SELECT 1")
        tables = list_fabric_tables()
        return {
            "status": "healthy",
            "tables_found": len(tables),
            "features": ["ChatGPT-like responses", "Automatic calculations", "Business insights", "Smart visualization"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/fabric/capabilities")
def get_capabilities():
    """Get enhanced system capabilities"""
    return {
        "conversation_style": "ChatGPT-like natural responses",
        "calculation_features": [
            "SQL-based statistical analysis",
            "Built-in aggregation and percentages",
            "Dynamic risk categorization",
            "Trend analysis through SQL",
            "Comparative analysis with GROUP BY",
            "Real-time metric calculations"
        ],
        "intelligence_features": [
            "Natural language understanding",
            "Context-aware responses",
            "Proactive suggestions",
            "Step-by-step explanations",
            "Business insight generation"
        ],
        "visualization_features": [
            "Smart visualization (automatically decides when to plot)",
            "Bar charts for comparisons",
            "Line charts for trends",
            "Pie charts for distributions",
            "Triggered by aggregations, comparisons, or trends in data"
        ],
        "supported_analysis": [
            "Cyber risk assessments (employee & device)",
            "Vulnerability management (CVSS scoring)",
            "Patch status monitoring",
            "Compliance monitoring",
            "Performance metrics",
            "Trend analysis",
            "Comparative studies",
            "Predictive insights"
        ]
    }
 
@app.delete("/api/fabric/clear-cache")
def clear_cache():
    """Clear the LRU cache for intelligent_analyze"""
    try:
        cached_intelligent_analyze.cache_clear()
        return {
            "status": "success",
            "message": "Cache cleared successfully",
            "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")
    
if __name__ == "__main__":
    print("🤖 ChatGPT-Style Intelligent Fabric Analytics")
    print("🧮 Advanced Calculation Engine")
    print("📊 Smart Visualization (Auto-Detects When to Plot)")
    print("💡 Natural Conversation Interface")
    print("")
    print("✨ ENHANCED FEATURES:")
    print("• Natural ChatGPT-like responses")
    print("• Automatic calculations & statistics")
    print("• Step-by-step explanations")
    print("• Business insights & recommendations")
    print("• Proactive follow-up suggestions")
    print("• Smart visualizations (bar, line, pie charts) based on query context")
    print("")
    print("💭 EXAMPLE QUESTIONS:")
    print('• "What is our average cyber risk score?"')
    print('• "Show me critical vulnerabilities (CVSS ≥9.0)"')  # Will plot if suitable
    print('• "How many vulnerabilities are unpatched by device type?"')  # Likely to plot
    print('• "Calculate compliance rates by department"')  # Likely to plot
    print('• "Show trends in login failures over time"')  # Will plot (line chart)
    print('• "Show me high-risk devices and explain why"')
    print("")
    print("🔗 API: POST /api/fabric/intelligent-analyze")
    print("🏥 Health: GET /api/fabric/health")
    print("📋 Info: GET /api/fabric/capabilities")
    print("")
    uvicorn.run("__main__:app", host="0.0.0.0", port=8002, reload=True)
