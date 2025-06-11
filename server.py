import asyncio 

import logging 

import re 

import time 

import uuid 

from datetime import datetime, date 

from decimal import Decimal 

from typing import List, Dict, Any, Optional 

from collections import OrderedDict 

 

import pyodbc 

import uvicorn 

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query 

from fastapi.middleware.cors import CORSMiddleware 

from openai import AsyncAzureOpenAI 

from pydantic import BaseModel, validator 

from sqlalchemy import create_engine, text 

from sqlalchemy.pool import QueuePool 

import os 

import json 

import pandas as pd 

from dotenv import load_dotenv 

from mcp.server.fastmcp import FastMCP 

import sqlparse 

import structlog 

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder 

from azure.kusto.data.exceptions import KustoServiceError 

 

# Load environment variables FIRST 

load_dotenv() 

 

# Setup structured logging 

structlog.configure( 

    processors=[structlog.processors.JSONRenderer()], 

    context_class=dict, 

    logger_factory=structlog.stdlib.LoggerFactory(), 

) 

logger = structlog.get_logger() 

 

def validate_environment(): 

    """Validate all required environment variables""" 

    required_vars = [ 

        "AZURE_OPENAI_API_KEY", 

        "AZURE_OPENAI_ENDPOINT",  

        "AZURE_OPENAI_DEPLOYMENT", 

        "FABRIC_SQL_ENDPOINT", 

        "FABRIC_DATABASE", 

        "FABRIC_CLIENT_ID", 

        "FABRIC_CLIENT_SECRET", 

        "KUSTO_CLUSTER", 

        "KUSTO_DATABASE",  

        "FABRIC_TENANT_ID" 

    ] 

     

    missing = [] 

    for var in required_vars: 

        value = os.getenv(var) 

        if not value or value.strip() == "": 

            missing.append(var) 

             

    if missing: 

        logger.error("Missing environment variables", missing=missing) 

        raise RuntimeError(f"Missing required environment variables: {missing}") 

     

    logger.info("Environment validation passed", total_vars=len(required_vars)) 

    return True 

 

# KQL connection 

 

def setup_kql_client(): 

    """Initialize KQL client with proper error handling""" 

    try: 

        # Validate environment first 

        validate_environment() 

         

        # Get KQL environment variables (strip whitespace just in case) 

        KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER", "").strip() 

        KUSTO_DATABASE = os.getenv("KUSTO_DATABASE", "").strip() 

        FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID", "").strip() 

        FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID", "").strip() 

        FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET", "").strip() 

         

        # Log configuration (without secrets) 

        logger.info("KQL configuration loaded",  

                   cluster=KUSTO_CLUSTER, 

                   database=KUSTO_DATABASE, 

                   tenant_id=FABRIC_TENANT_ID, 

                   client_id=FABRIC_CLIENT_ID, 

                   has_secret=bool(FABRIC_CLIENT_SECRET)) 

         

        # Validate KQL cluster URI format 

        if not KUSTO_CLUSTER.startswith("https://"): 

            raise ValueError(f"KUSTO_CLUSTER must start with 'https://'. Got: {KUSTO_CLUSTER}") 

             

        if "ingest" in KUSTO_CLUSTER.lower(): 

            logger.warning("KUSTO_CLUSTER appears to be an ingestion endpoint", cluster=KUSTO_CLUSTER) 

            raise ValueError( 

                f"KUSTO_CLUSTER should be a query endpoint, not ingestion. " 

                f"Expected format: https://<eventhouse>.<region>.kusto.fabric.microsoft.com" 

            ) 

         

        # Initialize KQL connection 

        kusto_connection_string = KustoConnectionStringBuilder.with_aad_application_key_authentication( 

            KUSTO_CLUSTER, 

            FABRIC_CLIENT_ID,  

            FABRIC_CLIENT_SECRET, 

            FABRIC_TENANT_ID 

        ) 

         

        kusto_client = KustoClient(kusto_connection_string) 

        logger.info("KQL client initialized successfully") 

         

        return kusto_client, KUSTO_DATABASE 

         

    except Exception as e: 

        logger.error("Failed to initialize KQL client", error=str(e)) 

        raise RuntimeError(f"KQL client initialization failed: {str(e)}") 

 

# Initialize KQL client and database 

try: 

    kusto_client, KUSTO_DATABASE = setup_kql_client() 

    logger.info("KQL setup completed", database=KUSTO_DATABASE) 

except Exception as e: 

    logger.error("Critical error during KQL setup", error=str(e)) 

    print(f"💥 KQL Setup Error: {e}") 

    print("🔧 Check your .env file and KQL database permissions") 

    exit(1) 

 

 

# Sessions creation and retrieval 

 

def generate_new_session_id(): 

    """Generate a new unique session ID for the current day""" 

    now = datetime.now() 

    date_string = now.strftime("%Y%m%d") 

    timestamp = int(time.time() * 1000)  # milliseconds for uniqueness 

    return f"powerbi_{date_string}_{timestamp}" 

 

def get_session_id_from_request(session: Optional[str] = None): 

    """Enhanced session management with multiple sessions per day""" 

    if session and (session.startswith('powerbi_') or session == 'new'): 

        if session == 'new': 

            # Generate a completely new session 

            return generate_new_session_id() 

        return session 

     

    # Default fallback 

    now = datetime.now() 

    date_string = now.strftime("%Y%m%d") 

    return f"powerbi_{date_string}_default" 

 

 

# Initialize KQL table if not exists 

async def initialize_kql_table(): 

    """Create ChatHistory table if it doesn't exist""" 

    create_table_query = """ 

    .create table ChatHistory ( 

        SessionID: string, 

        Timestamp: datetime, 

        ConversationID: string, 

        Question: string, 

        Response: string, 

        Context: string 

    ) 

    """ 

    try: 

        await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, create_table_query) 

        ) 

        logger.info("KQL table ChatHistory created or verified") 

    except KustoServiceError as e: 

        error_msg = str(e).lower() 

        if "already exists" in error_msg or "entityalreadyexists" in error_msg: 

            logger.info("KQL table ChatHistory already exists") 

        else: 

            logger.error("Failed to create KQL table", error=str(e)) 

            raise 

    except Exception as e: 

        logger.error("Unexpected error creating KQL table", error=str(e)) 

        raise 

 

# Test KQL connection 

async def test_kql_connection(): 

    """Test the KQL connection with a simple query""" 

    try: 

        test_query = "print 'KQL connection test successful'" 

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, test_query) 

        ) 

        logger.info("KQL connection test passed") 

        return True 

    except Exception as e: 

        logger.error("KQL connection test failed", error=str(e)) 

        return False 

 

# Store query and response in KQL 

 

async def store_in_kql(question: str, response: Dict, context: List[Dict], session_id: str = None): 

    """Fixed KQL storage with correct datetime format""" 

     

    # Skip storing schema-related queries 

    if question.lower() in ['tables_info', 'schema_info'] or 'tables_info' in str(response): 

        logger.info("Skipping KQL storage for schema query") 

        return 

     

    # Use provided session ID or fall back to fixed session 

    actual_session_id = session_id if session_id else "default-session-1234567890" 

     

    conversation_id = str(uuid.uuid4()) 

    # Use proper datetime format for KQL 

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") 

     

    try: 

        # Safely serialize the response and context 

        response_json = json.dumps(response, default=safe_json_serialize) 

        context_json = json.dumps(context, default=safe_json_serialize) 

         

        print(f"\n🔍 STORAGE DEBUG:") 

        print(f"  SessionID: {actual_session_id}") 

        print(f"  Timestamp: {timestamp}") 

        print(f"  Question: {question[:50]}...") 

        print(f"  Response JSON length: {len(response_json)}") 

         

        # Clean data for KQL 

        clean_session_id = actual_session_id.replace('"', '').replace("'", "") 

        clean_question = question.replace('"', '""').replace('\n', ' ').replace('\r', ' ') 

        clean_response = response_json.replace('"', '""').replace('\n', ' ').replace('\r', ' ') 

        clean_context = context_json.replace('"', '""').replace('\n', ' ').replace('\r', ' ') 

         

        # Use correct KQL syntax with datetime() function - NO COLUMN HEADERS 

        ingest_query = f'''.ingest inline into table ChatHistory <| 

"{clean_session_id}",datetime({timestamp}),"{conversation_id}","{clean_question}","{clean_response}","{clean_context}"''' 

         

        print(f"  Executing KQL ingest...") 

        print(f"  Query: {ingest_query[:200]}...") 

         

        # Execute the ingest 

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, ingest_query) 

        ) 

         

        print(f"  ✅ KQL execution completed") 

         

        # Verify the insert worked 

        verify_query = f'ChatHistory | where ConversationID == "{conversation_id}" | count' 

        verify_result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, verify_query) 

        ) 

         

        count = verify_result.primary_results[0][0]["Count"] if verify_result.primary_results[0] else 0 

        print(f"  ✅ Verification: {count} record(s) inserted for conversation {conversation_id}") 

         

        if count > 0: 

            logger.info("Stored in KQL successfully", session_id=actual_session_id, conversation_id=conversation_id) 

        else: 

            logger.warning("KQL insert may have failed - no records found", conversation_id=conversation_id) 

         

    except Exception as e: 

        print(f"  ❌ KQL storage failed: {str(e)}") 

        logger.error("Failed to store in KQL", session_id=actual_session_id, error=str(e), conversation_id=conversation_id) 

        print(f"  💡 Continuing without KQL storage...") 

        return 

 

 

# Retrieve cached response from KQL 

async def get_from_kql_cache(question: str, session_id: str = None) -> Optional[Dict]: 

    """Modified to support dynamic sessions""" 

    actual_session_id = session_id if session_id else "default-session-1234567890" 

    normalized_question = normalize_question(question) 

     

    cache_query = f""" 

    ChatHistory 

    | where SessionID == '{actual_session_id}' 

    | where Question == '{normalized_question.replace("'", "''")}' 

    | project Response 

    | take 1 

    """ 

    try: 

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, cache_query) 

        ) 

        if result.primary_results and len(result.primary_results[0]) > 0: 

            response = json.loads(result.primary_results[0][0]["Response"]) 

            response["session_id"] = actual_session_id 

            logger.info("KQL cache hit", question=normalized_question, session_id=actual_session_id) 

            return response 

        return None 

    except Exception as e: 

        logger.error("KQL cache retrieval failed", error=str(e), session_id=actual_session_id) 

        return None 

 

# Retrieve latest 10 responses for UI 

async def get_latest_responses(session_id: str = None) -> List[Dict]: 

    """Updated to support dynamic sessions""" 

    actual_session_id = session_id if session_id else "default-session-1234567890" 

     

    history_query = f""" 

    ChatHistory 

    | where SessionID == '{actual_session_id}' 

    | order by Timestamp desc 

    | take 10 

    | project Timestamp, Question, Response 

    """ 

    try: 

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, history_query) 

        ) 

        responses = [] 

        for row in result.primary_results[0]: 

            response = json.loads(row["Response"]) 

            responses.append({ 

                "timestamp": row["Timestamp"], 

                "question": row["Question"], 

                "response": response 

            }) 

        logger.info("Retrieved latest responses", count=len(responses)) 

        return responses 

    except Exception as e: 

        logger.error("Failed to retrieve latest responses", error=str(e)) 

        return [] 

 

# Normalize question for better cache hits 

def normalize_question(question: str) -> str: 

    question = question.lower().strip() 

    question = re.sub(r'\s+', ' ', question) 

    return question 

 

def safe_json_serialize(obj): 

    """Safe JSON serialization for various data types""" 

    if isinstance(obj, (datetime, date)): 

        return obj.isoformat() 

    elif isinstance(obj, Decimal): 

        return float(obj) 

    elif hasattr(obj, '__dict__'): 

        return str(obj) 

    return obj 

 

def extract_context_from_results(results: List[Dict[str, Any]]) -> Dict[str, Any]: 

    context = {} 

    if not results: 

        return context 

    sample_row = results[0] 

    id_columns = [col for col in sample_row.keys() if col.lower().endswith('id')] 

    for id_col in id_columns: 

        unique_ids = list(set(str(row[id_col]) for row in results if row.get(id_col))) 

        if unique_ids: 

            context[f'{id_col.lower()}_list'] = unique_ids[:50] 

    identifier_patterns = ['name', 'code', 'key', 'reference'] 

    for pattern in identifier_patterns: 

        matching_cols = [col for col in sample_row.keys() if pattern in col.lower()] 

        for col in matching_cols: 

            unique_values = list(set(str(row[col]) for row in results if row.get(col) and str(row[col]).strip())) 

            if unique_values and len(unique_values) <= 50: 

                context[f'{col.lower()}_list'] = unique_values[:20] 

    return context 

 

class IntentAnalyzer: 

    @staticmethod 

    async def analyze(question: str, conversation_history: List[Dict], context: Dict) -> Dict[str, Any]: 

        recent_context = "" 

        if conversation_history: 

            last_user_msg = next((msg.get('content', '') for msg in reversed(conversation_history) if msg.get('role') == 'user'), '') 

            last_assistant_msg = next((msg.get('content', '') for msg in reversed(conversation_history) if msg.get('role') == 'assistant'), '') 

            if last_user_msg: 

                recent_context = f"Previous question: {last_user_msg}\n" 

            if context: 

                context_summary = {key: f"{len(values)} items (sample: {values[:3]})" for key, values in context.items() if key.endswith('_list')} 

                recent_context += f"Previous query results: {context_summary}\n" 

         

        intent_prompt = f"""You are an expert data analyst. Analyze the user's question and determine how to handle it. 

 

CONVERSATION CONTEXT: 

{recent_context} 

 

CURRENT QUESTION: "{question}" 

 

Analyze this question and respond with a JSON object containing: 

1. "question_type": "follow_up", "new_analysis", or "clarification" 

2. "reasoning": Brief explanation 

3. "context_relevance": "use_all", "use_partial", or "ignore" 

4. "focus_entities": Primary focus (employees, devices, incidents, etc.) 

 

Respond with valid JSON only.""" 

        try: 

            response = await ask_intelligent_llm_async(intent_prompt) 

            response_clean = response.strip().lstrip('```json').rstrip('```').strip() 

            intent_analysis = json.loads(response_clean) 

            required_fields = ['question_type', 'reasoning', 'context_relevance', 'focus_entities'] 

            if all(field in intent_analysis for field in required_fields): 

                return intent_analysis 

            logger.warning("Intent analysis missing required fields", response=intent_analysis) 

            return {"question_type": "new_analysis", "context_relevance": "ignore", "reasoning": "Fallback due to parsing error", "focus_entities": "unknown"} 

        except Exception as e: 

            logger.error("Intent analysis failed", error=str(e)) 

            question_lower = question.lower() 

            if any(word in question_lower for word in ['their', 'those', 'these', 'same', 'above']): 

                return {"question_type": "follow_up", "context_relevance": "use_all", "reasoning": "Fallback: detected referential language", "focus_entities": "same_as_previous"} 

            return {"question_type": "new_analysis", "context_relevance": "ignore", "reasoning": "Fallback: appears to be new analysis", "focus_entities": "unknown"} 

 

async def add_smart_context_to_prompt(base_prompt: str, conversation_history: List[Dict], question: str) -> str: 

    if not conversation_history: 

        return base_prompt 

    last_context = next((item.get('context', {}) for item in reversed(conversation_history) if item.get('role') == 'assistant' and item.get('context')), {}) 

    if not last_context: 

        return base_prompt 

    intent_analysis = await IntentAnalyzer.analyze(question, conversation_history, last_context) 

    logger.info("Intent analysis", analysis=intent_analysis) 

    if intent_analysis['context_relevance'] == 'ignore': 

        return base_prompt + f"\n\nCONTEXT GUIDANCE: This appears to be a {intent_analysis['question_type']} question. {intent_analysis['reasoning']} Therefore, analyze the FULL dataset - do NOT filter by previous query results." 

    context_text = f"\n\nINTELLIGENT CONTEXT ANALYSIS: {intent_analysis['reasoning']}" 

    for key, values in last_context.items(): 

        if key.endswith('_list') and values: 

            column_base = key.replace('_list', '') 

            column_name = column_base.upper() if column_base.endswith('id') else column_base 

            total_count = len(values) 

            sample_values = values[:5] 

            value_list = "', '".join(str(v) for v in values) 

            context_text += f"\n\nPREVIOUS QUERY RESULTS: Found {total_count} {column_base} values (sample: {sample_values})" 

            context_text += f"\nFor this {intent_analysis['question_type']} question about {intent_analysis['focus_entities']}, filter using: WHERE [{column_name}] IN ('{value_list}')" 

    context_text += f"\n\nThis is a {intent_analysis['question_type'].upper()} question - use the context above to filter your query appropriately." 

    return base_prompt + context_text 

 

# Load environment variables 

load_dotenv() 

 

# FastAPI app 

app = FastAPI( 

    title="Intelligent Microsoft Fabric SQL Analytics", 

    description="Processes natural language questions to generate SQL queries, execute them, and provide insights with optional visualizations." 

) 

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"], expose_headers=["*"]) 

 

# Rate limiting 

from slowapi import Limiter, _rate_limit_exceeded_handler 

from slowapi.util import get_remote_address 

 

limiter = Limiter(key_func=get_remote_address) 

app.state.limiter = limiter 

app.add_exception_handler(429, _rate_limit_exceeded_handler) 

 

# MCP server 

mcp = FastMCP("Intelligent Fabric Analytics", dependencies=["pyodbc", "fastapi", "python-dotenv", "pandas", "sqlalchemy", "slowapi", "sqlparse", "structlog", "azure-kusto-data"]) 

 

# Azure OpenAI client (async) 

client = AsyncAzureOpenAI( 

    api_key=os.getenv("AZURE_OPENAI_API_KEY"), 

    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"), 

    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT") 

) 

 

# Database connection pooling 

connection_string = ( 

    f"Driver={{ODBC Driver 18 for SQL Server}};" 

    f"Server={os.getenv('FABRIC_SQL_ENDPOINT')};" 

    f"Database={os.getenv('FABRIC_DATABASE')};" 

    f"Authentication=ActiveDirectoryServicePrincipal;" 

    f"UID={os.getenv('FABRIC_CLIENT_ID')};" 

    f"PWD={os.getenv('FABRIC_CLIENT_SECRET')};" 

    f"Encrypt=yes;" 

    f"TrustServerCertificate=no;" 

) 

engine = create_engine( 

    f"mssql+pyodbc:///?odbc_connect={connection_string}", 

    poolclass=QueuePool, 

    pool_size=5, 

    max_overflow=10, 

    pool_timeout=30, 

    pool_recycle=3600 

) 

 

def execute_query(query: str, params=None) -> List[Dict[str, Any]]: 

    """Enhanced execute_query with better GROUP BY error messages""" 

    try: 

        with engine.connect() as conn: 

            executable_query = text(query) 

            cursor = conn.execute(executable_query, params or {}) 

            columns = cursor.keys() 

            rows = cursor.fetchall() 

            result = [] 

            for row in rows: 

                row_dict = dict(zip(columns, row)) 

                for key, value in row_dict.items(): 

                    if isinstance(value, datetime): 

                        row_dict[key] = value.isoformat() 

                    elif hasattr(value, 'date') and callable(getattr(value, 'date')): 

                        row_dict[key] = value.isoformat() 

                    elif isinstance(value, (bytes, bytearray)): 

                        row_dict[key] = value.decode('utf-8', errors='ignore') 

                    elif isinstance(value, Decimal): 

                        row_dict[key] = float(value) 

                result.append(row_dict) 

        return result 

    except Exception as e: 

        error_str = str(e) 

        logger.error("Query execution error", query=query, params=params, error=str(e)) 

         

        # Improved error handling for GROUP BY issues 

        if "8120" in error_str or "GROUP BY" in error_str: 

            if "is not contained in either an aggregate function or the GROUP BY clause" in error_str: 

                raise Exception( 

                    "SQL GROUP BY error: All non-aggregate columns in SELECT must be included in GROUP BY clause. " 

                    "Fix: Add missing columns to GROUP BY, or use aggregate functions like COUNT(), SUM(), AVG() for calculated fields. " 

                    f"Original error: {error_str}" 

                ) 

            else: 

                raise Exception( 

                    "SQL GROUP BY error: When using GROUP BY, all SELECT columns must either be in the GROUP BY clause " 

                    "or use aggregate functions (COUNT, SUM, AVG, etc.). " 

                    f"Original error: {error_str}" 

                ) 

        else: 

            raise 

 

# Global variable to cache schema in memory 

CACHED_TABLES_INFO = None 

SCHEMA_CACHE_TIMESTAMP = None 

SCHEMA_CACHE_DURATION =  864000 # Cache for 1 hour 

 

async def get_cached_tables_info(): 

    """Get schema from memory cache - NO KQL storage for schema""" 

    global CACHED_TABLES_INFO, SCHEMA_CACHE_TIMESTAMP 

     

    current_time = time.time() 

     

     

     

    # Check if we have valid cached data 

    if (CACHED_TABLES_INFO is not None and  

        SCHEMA_CACHE_TIMESTAMP is not None and  

        (current_time - SCHEMA_CACHE_TIMESTAMP) < SCHEMA_CACHE_DURATION): 

        logger.info("Schema cache hit from memory") 

        return CACHED_TABLES_INFO 

     

    # Cache is empty or expired, fetch fresh data 

    logger.info("Fetching fresh schema data") 

    start_time = time.time() 

     

    try: 

        tables_info = await get_tables_info() 

         

        # Cache in memory only - NOT in KQL 

        CACHED_TABLES_INFO = tables_info 

        SCHEMA_CACHE_TIMESTAMP = current_time 

         

        duration = time.time() - start_time 

        logger.info("Schema fetched and cached in memory",  

                   duration=duration,  

                   table_count=len(tables_info)) 

         

        return tables_info 

         

    except Exception as e: 

        logger.error("Failed to fetch schema", error=str(e)) 

        # Return cached data if available, even if expired 

        if CACHED_TABLES_INFO is not None: 

            logger.warning("Using expired schema cache due to fetch error") 

            return CACHED_TABLES_INFO 

        raise 

 

async def preload_schema(): 

    """Preload schema during application startup""" 

    try: 

        logger.info("Preloading database schema...") 

        start_time = time.time() 

         

        tables_info = await get_cached_tables_info() 

         

        duration = time.time() - start_time 

        logger.info("Schema preloaded successfully",  

                   duration=duration, 

                   table_count=len(tables_info), 

                   total_columns=sum(len(t.get('columns', [])) for t in tables_info)) 

         

        return True 

         

    except Exception as e: 

        logger.error("Schema preload failed", error=str(e)) 

        print(f"⚠️  Schema preload failed: {e}") 

        print("    App will still work, but first query may be slower") 

        return False 

     

async def get_tables_info(): 

    loop = asyncio.get_event_loop() 

    query = """ 

    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE 

    FROM INFORMATION_SCHEMA.TABLES 

    WHERE TABLE_TYPE = 'BASE TABLE' 

    AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA') 

    ORDER BY TABLE_SCHEMA, TABLE_NAME 

    """ 

    tables = await loop.run_in_executor(None, lambda: execute_query(query)) 

    tables_info = [] 

 

    async def fetch_table_metadata(table): 

        column_query = """ 

        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH 

        FROM INFORMATION_SCHEMA.COLUMNS 

        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table 

        ORDER BY ORDINAL_POSITION 

        """ 

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

        WHERE C.TABLE_SCHEMA = :schema AND C.TABLE_NAME = :table 

        """ 

        sample_query = f"SELECT TOP 3 * FROM [{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}]" 

         

        columns, fks, sample_data = await asyncio.gather( 

            loop.run_in_executor(None, lambda: execute_query(column_query, {"schema": table["TABLE_SCHEMA"], "table": table["TABLE_NAME"]})), 

            loop.run_in_executor(None, lambda: execute_query(fk_query, {"schema": table["TABLE_SCHEMA"], "table": table["TABLE_NAME"]})), 

            loop.run_in_executor(None, lambda: execute_query(sample_query)), 

            return_exceptions=True 

        ) 

         

        if isinstance(columns, Exception) or isinstance(fks, Exception) or isinstance(sample_data, Exception): 

            logger.warning("Failed to fetch metadata for table", table=table["TABLE_NAME"], error=str(columns or fks or sample_data)) 

            return None 

         

        fk_info = [f"{fk['COLUMN_NAME']} references {fk['REFERENCED_TABLE']}.{fk['REFERENCED_COLUMN']}" for fk in fks] 

        enhanced_columns = [] 

        numeric_columns = [] 

        text_columns = [] 

        date_columns = [] 

        column_values = {} 

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

                try: 

                    distinct_query = f"SELECT DISTINCT TOP 10 [{col_name}] FROM [{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}] WHERE [{col_name}] IS NOT NULL" 

                    distinct_values = await loop.run_in_executor(None, lambda: execute_query(distinct_query)) 

                    column_values[col_name] = [row[col_name] for row in distinct_values] 

                except: 

                    column_values[col_name] = [] 

            elif data_type in ['datetime', 'datetime2', 'date', 'time', 'datetimeoffset', 'smalldatetime']: 

                date_columns.append(col_name) 

                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - DATE: Use MAX(), MIN(), date functions") 

            else: 

                enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable})") 

        return { 

            "table": f"[{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}]", 

            "columns": enhanced_columns, 

            "numeric_columns": numeric_columns, 

            "text_columns": text_columns, 

            "date_columns": date_columns, 

            "foreign_keys": fk_info, 

            "sample_data": sample_data[:2] if sample_data else [], 

            "column_values": column_values 

        } 

 

    tables_info = await asyncio.gather(*(fetch_table_metadata(table) for table in tables)) 

    return [info for info in tables_info if info] 

 

def clean_generated_sql(sql_text: str) -> str: 

    """Enhanced SQL cleaning with better validation""" 

    if not sql_text: 

        return "" 

         

    sql = sql_text.strip() 

     

    # Remove code block markers 

    if sql.startswith('```'): 

        lines = sql.split('\n') 

        start_idx = 1 if lines[0].startswith('```') else 0 

        end_idx = len(lines) 

        for i, line in enumerate(lines[1:], 1): 

            if line.strip().startswith('```'): 

                end_idx = i 

                break 

        sql = '\n'.join(lines[start_idx:end_idx]) 

     

    # Clean up the SQL 

    lines = sql.split('\n') 

    sql_lines = [] 

    in_select = False 

     

    for line in lines: 

        line = line.strip() 

        if not line or line.startswith('--'): 

            continue 

             

        # Start of a SELECT statement 

        if line.upper().startswith('SELECT'): 

            in_select = True 

            sql_lines = [line] 

        elif in_select: 

            # Valid SQL keywords and constructs 

            if any(keyword in line.upper() for keyword in 

                  ['FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'GROUP', 'HAVING', 'ORDER', 'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']): 

                sql_lines.append(line) 

            elif line.upper().startswith('SELECT'): 

                # New SELECT statement, stop here 

                break 

            elif any(char in line for char in ['[', ']', '.', ',', '(', ')', '=', '<', '>', '!', "'", '"']): 

                # Looks like SQL content 

                sql_lines.append(line) 

            else: 

                # Doesn't look like SQL, might be end of query 

                if not any(char in line for char in ['[', ']', '.', ',']): 

                    break 

                sql_lines.append(line) 

     

    # Join and clean up 

    sql = ' '.join(sql_lines).strip().rstrip(';').rstrip(',') 

     

    # Basic validation 

    if sql: 

        sql_upper = sql.upper() 

         

        # Must start with SELECT and contain FROM 

        if not sql_upper.startswith('SELECT') or 'FROM' not in sql_upper: 

            return "" 

             

        # Check for obvious issues 

        if any(issue in sql_upper for issue in ['FROM FROM', ', FROM', 'SELECT FROM', 'WHERE FROM']): 

            return "" 

             

        # Check for incomplete statements 

        if any(sql_upper.endswith(keyword) for keyword in ['FROM', 'SELECT', 'WHERE', 'AND', 'OR', 'JOIN', 'ON']): 

            return "" 

     

    return sql 

 

def sanitize_sql(sql: str) -> str: 

    """Enhanced SQL sanitization""" 

    try: 

        parsed = sqlparse.parse(sql)[0] 

         

        # Allowed keywords for security 

        allowed_keywords = { 

            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 

            'GROUP', 'BY', 'ORDER', 'HAVING', 'AND', 'OR', 'ON', 'AS', 'IN', 'NOT', 

            'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 

            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'TOP', 'DISTINCT', 'CAST', 'CONVERT', 

            'DATEPART', 'DATEADD', 'DATEDIFF', 'GETDATE', 'YEAR', 'MONTH', 'DAY' 

        } 

         

        # Check for dangerous keywords 

        dangerous_keywords = { 

            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', 

            'EXEC', 'EXECUTE', 'SP_', 'XP_', 'OPENROWSET', 'OPENDATASOURCE' 

        } 

         

        sql_upper = sql.upper() 

        for keyword in dangerous_keywords: 

            if keyword in sql_upper: 

                raise ValueError(f"Dangerous SQL keyword detected: {keyword}") 

         

        # Basic token validation 

        for token in parsed.tokens: 

            if token.ttype is sqlparse.tokens.Keyword: 

                token_value = token.value.upper() 

                if token_value not in allowed_keywords and token_value not in ['AS', 'BY']: 

                    # Allow some additional common SQL constructs 

                    if token_value not in ['ASC', 'DESC', 'INNER', 'OUTER']: 

                        logger.warning(f"Potentially unsupported SQL keyword: {token_value}") 

         

        return str(parsed) 

         

    except Exception as e: 

        logger.error("SQL sanitization failed", error=str(e)) 

        raise ValueError(f"SQL sanitization failed: {str(e)}") 

 

def should_generate_visualization(question: str, sql: str, results: List[Dict[str, Any]]) -> bool: 

    """Enhanced visualization detection""" 

    if not results or len(results) < 1: 

        return False 

     

    # More comprehensive keyword detection 

    chart_keywords = [ 

        "chart", "graph", "visualize", "plot", "display", "show",  

        "trend", "distribution", "compare", "comparison", "percentage",  

        "over time", "by", "breakdown", "analysis", "visual", 

        "bar chart", "pie chart", "line chart", "histogram" 

    ] 

     

    # Check question for visualization intent 

    question_lower = question.lower() 

    has_viz_keywords = any(keyword in question_lower for keyword in chart_keywords) 

     

    # Check if data is suitable for visualization 

    if len(results) > 100:  # Too many data points 

        return False 

         

    columns = list(results[0].keys()) 

    numeric_cols = [] 

    categorical_cols = [] 

     

    # Better column type detection 

    for col in columns: 

        sample_values = [row[col] for row in results[:5] if row[col] is not None] 

        if sample_values: 

            if any(isinstance(val, (int, float)) for val in sample_values): 

                numeric_cols.append(col) 

            elif any(isinstance(val, str) for val in sample_values): 

                categorical_cols.append(col) 

     

    # Need at least one numeric and one categorical column, OR aggregated data 

    has_suitable_data = (len(numeric_cols) >= 1 and len(categorical_cols) >= 1) or len(results) <= 20 

     

    # Always generate chart if explicitly requested 

    explicit_chart_request = any(word in question_lower for word in ["chart", "graph", "plot", "visualize"]) 

     

    return explicit_chart_request or (has_viz_keywords and has_suitable_data) 

 

async def generate_visualization(question: str, results: List[Dict], sql: str) -> Optional[Dict]: 

    """Enhanced visualization generation with explanatory text""" 

    if not results: 

        return None 

     

    try: 

        # Analyze the best chart type 

        prompt = f""" 

        Analyze this data and recommend the best Chart.js configuration: 

         

        Question: {question} 

        Data Sample: {json.dumps(results[:3], default=safe_json_serialize)} 

        Total Records: {len(results)} 

         

        Choose the best chart type from: bar, line, pie, doughnut 

         

        Rules: 

        - Use 'bar' for comparisons, counts, categories 

        - Use 'line' for trends over time 

        - Use 'pie' for distributions, percentages, parts of whole 

        - Use 'doughnut' for proportions with emphasis on total 

         

        Respond with JSON only: 

        {{ 

            "chart_type": "recommended_type", 

            "reasoning": "brief explanation" 

        }} 

        """ 

         

        response = await ask_intelligent_llm_async(prompt) 

        chart_analysis = json.loads(response.strip().lstrip('```json').rstrip('```').strip()) 

        chart_type = chart_analysis.get("chart_type", "bar") 

         

    except Exception as e: 

        logger.warning("Chart type analysis failed, using fallback", error=str(e)) 

        # Fallback logic 

        question_lower = question.lower() 

        if any(word in question_lower for word in ["trend", "over time", "timeline"]): 

            chart_type = "line" 

        elif any(word in question_lower for word in ["distribution", "percentage", "proportion"]): 

            chart_type = "pie" 

        else: 

            chart_type = "bar" 

     

    # Prepare data for chart 

    columns = list(results[0].keys()) 

     

    # Find the best label and value columns 

    numeric_cols = [] 

    categorical_cols = [] 

     

    for col in columns: 

        sample_values = [row[col] for row in results[:5] if row[col] is not None] 

        if sample_values: 

            if all(isinstance(val, (int, float)) for val in sample_values): 

                numeric_cols.append(col) 

            else: 

                categorical_cols.append(col) 

     

    if not numeric_cols: 

        return None 

     

    # Choose label column (categorical first, then first column) 

    label_col = categorical_cols[0] if categorical_cols else columns[0] 

    value_col = numeric_cols[0] 

     

    # Limit data points for better visualization 

    chart_data = results[:20]  # Limit to 20 points max 

     

    # Extract labels and values 

    labels = [] 

    values = [] 

     

    for row in chart_data: 

        label = str(row.get(label_col, 'Unknown'))[:30]  # Truncate long labels 

        value = row.get(value_col, 0) 

         

        # Convert value to number 

        try: 

            if isinstance(value, str): 

                value = float(value) if '.' in value else int(value) 

            elif value is None: 

                value = 0 

        except: 

            value = 0 

             

        labels.append(label) 

        values.append(value) 

     

    # Generate chart explanation 

    total_value = sum(values) 

    max_value = max(values) if values else 0 

    min_value = min(values) if values else 0 

    max_index = values.index(max_value) if values else 0 

    min_index = values.index(min_value) if values else 0 

     

    # Create contextual explanation based on chart type 

    if chart_type in ["pie", "doughnut"]: 

        max_percentage = (max_value / total_value * 100) if total_value > 0 else 0 

        explanation = f"This {chart_type} chart shows the distribution of {value_col.replace('_', ' ').lower()} across different {label_col.replace('_', ' ').lower()}. " 

        explanation += f"The largest segment is '{labels[max_index]}' with {max_value:,.0f} ({max_percentage:.1f}% of total). " 

        explanation += f"Total across all categories: {total_value:,.0f}." 

     

    elif chart_type == "line": 

        explanation = f"This line chart displays the trend of {value_col.replace('_', ' ').lower()} over {label_col.replace('_', ' ').lower()}. " 

        if len(values) > 1: 

            trend = "increasing" if values[-1] > values[0] else "decreasing" if values[-1] < values[0] else "stable" 

            explanation += f"The overall trend appears to be {trend}. " 

        explanation += f"Peak value: {max_value:,.0f} at '{labels[max_index]}', lowest: {min_value:,.0f} at '{labels[min_index]}'." 

     

    else:  # bar chart 

        explanation = f"This bar chart compares {value_col.replace('_', ' ').lower()} across different {label_col.replace('_', ' ').lower()}. " 

        explanation += f"'{labels[max_index]}' has the highest value at {max_value:,.0f}, while '{labels[min_index]}' has the lowest at {min_value:,.0f}. " 

        if len(values) > 2: 

            avg_value = sum(values) / len(values) 

            explanation += f"Average value: {avg_value:,.1f}." 

     

    # Create Chart.js configuration 

    chart_config = { 

        "type": chart_type, 

        "data": { 

            "labels": labels, 

            "datasets": [{ 

                "label": value_col.replace('_', ' ').title(), 

                "data": values, 

                "backgroundColor": [ 

                    "rgba(75, 192, 192, 0.8)", 

                    "rgba(255, 99, 132, 0.8)",  

                    "rgba(54, 162, 235, 0.8)", 

                    "rgba(255, 206, 86, 0.8)", 

                    "rgba(153, 102, 255, 0.8)", 

                    "rgba(255, 159, 64, 0.8)", 

                    "rgba(199, 199, 199, 0.8)", 

                    "rgba(83, 102, 255, 0.8)", 

                    "rgba(255, 99, 71, 0.8)", 

                    "rgba(50, 205, 50, 0.8)" 

                ][:len(values)], 

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

                ][:len(values)], 

                "borderWidth": 2 

            }] 

        }, 

        "options": { 

            "responsive": True, 

            "maintainAspectRatio": False, 

            "plugins": { 

                "title": { 

                    "display": True, 

                    "text": f"{value_col.replace('_', ' ').title()} by {label_col.replace('_', ' ').title()}", 

                    "font": {"size": 16, "weight": "bold"} 

                }, 

                "legend": { 

                    "display": chart_type in ["pie", "doughnut"] 

                } 

            } 

        } 

    } 

     

    # Add scales for non-pie charts 

    if chart_type not in ["pie", "doughnut"]: 

        chart_config["options"]["scales"] = { 

            "y": { 

                "beginAtZero": True, 

                "title": { 

                    "display": True, 

                    "text": value_col.replace('_', ' ').title() 

                } 

            }, 

            "x": { 

                "title": { 

                    "display": True, 

                    "text": label_col.replace('_', ' ').title() 

                } 

            } 

        } 

     

    # Return both chart config and explanation 

    return { 

        "chart_config": chart_config, 

        "explanation": explanation, 

        "chart_type": chart_type, 

        "data_points": len(values), 

        "total_value": total_value if chart_type in ["pie", "doughnut"] else None 

    } 

 

 

def extract_chart_from_llm_response(llm_response: str) -> Optional[Dict]: 

    """Extract Chart.js config from LLM response""" 

    try: 

        # Look for chartjs code blocks 

        if "```chartjs" in llm_response: 

            start = llm_response.find("```chartjs") + 10 

            end = llm_response.find("```", start) 

            if end > start: 

                chart_json = llm_response[start:end].strip() 

                return json.loads(chart_json) 

    except Exception as e: 

        logger.warning("Failed to extract chart from LLM response", error=str(e)) 

     

    return None 

 

async def add_visualization_to_response(question: str, sql: str, results: List[Dict], response: Dict): 

    """Add visualization with explanation to response if appropriate""" 

    try: 

        if should_generate_visualization(question, sql, results): 

            logger.info("Generating visualization", question=question, result_count=len(results)) 

             

            # Generate chart config and explanation 

            viz_data = await generate_visualization(question, results, sql) 

             

            if viz_data: 

                response["visualization"] = viz_data["chart_config"] 

                response["chart_explanation"] = viz_data["explanation"] 

                response["chart_type"] = viz_data["chart_type"] 

                response["has_visualization"] = True 

                logger.info("Visualization added to response with explanation") 

                 

                # Enhance the main analysis with chart context 

                if "analysis" in response: 

                    response["analysis"] += f"\n\n**📊 Chart Insights:**\n{viz_data['explanation']}" 

            else: 

                logger.warning("Failed to generate chart config") 

        else: 

            logger.info("No visualization needed", question=question) 

             

    except Exception as e: 

        logger.error("Visualization generation failed", error=str(e)) 

 

def filter_schema_for_question(question: str, tables_info: List[Dict]) -> List[Dict]: 

    if not tables_info: 

        return [] 

    question_lower = question.lower().strip() 

    question_terms = set(term for term in question_lower.split() if len(term) > 2) 

    relevant_tables = [] 

    for table_info in tables_info: 

        table_name = table_info['table'].lower() 

        table_base_name = table_name.split('.')[-1].strip('[]') 

        columns = [col.lower() for col in table_info.get('columns', [])] 

        table_terms = set([table_base_name] + [col.split()[0] for col in columns]) 

        if question_terms.intersection(table_terms) or any(term in table_base_name or any(term in col for col in columns) for term in question_terms): 

            relevant_tables.append(table_info) 

    return relevant_tables[:6] or tables_info 

 

 

def load_base_prompt(): 

    """Updated base prompt with proper boolean/bit column handling""" 

    return """You are an expert SQL analyst. You must respond in this EXACT format: 

  

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

 

**BOOLEAN/BIT COLUMN RULES (VERY IMPORTANT):** 

8. For boolean/bit columns (True/False, 1/0 values like EndpointProtection, IsEncrypted): 

   - For EFFECTIVENESS/PERCENTAGE questions: Use CAST([column] AS INT) or CASE WHEN [column] = 'True' THEN 1 ELSE 0 END 

   - CORRECT: SUM(CASE WHEN [EndpointProtection] = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS EffectivenessPercent 

   - CORRECT: SUM(CAST([EndpointProtection] AS INT)) * 100.0 / COUNT(*) AS EffectivenessPercent   

   - For RAW DATA questions: SELECT [column] directly with WHERE clause 

   - EXAMPLE: WHERE [EndpointProtection] = 'True' for enabled devices 

   - NEVER use AVG() directly on boolean columns without conversion 

 

9. For varchar columns, use COUNT() or GROUP BY, not AVG() 

 

**BOOLEAN COLUMN EXAMPLES:** 

- Effectiveness: SUM(CASE WHEN [EndpointProtection] = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) 

- Count of protected: SUM(CASE WHEN [EndpointProtection] = 'True' THEN 1 ELSE 0 END) 

- Count of unprotected: SUM(CASE WHEN [EndpointProtection] = 'False' THEN 1 ELSE 0 END) 

- Filter for protected only: WHERE [EndpointProtection] = 'True' 

 

TEMPORAL/DATE QUERY RULES: 

10. When using DATEPART() in SELECT, you MUST include the SAME DATEPART() expressions in GROUP BY 

11. CORRECT: SELECT DATEPART(MONTH, [Date]) AS Month FROM table GROUP BY DATEPART(MONTH, [Date]) 

12. WRONG: SELECT DATEPART(MONTH, [Date]) AS Month FROM table GROUP BY [Date] 

13. For time-based analysis, always filter recent data: WHERE [DateColumn] >= DATEADD(MONTH, -6, GETDATE()) 

14. For trends over time, use: GROUP BY DATEPART(YEAR, [Date]), DATEPART(MONTH, [Date]) 

15. Order temporal results by: ORDER BY DATEPART(YEAR, [Date]), DATEPART(MONTH, [Date]) 

     

CHART GENERATION RULES: 

- Charts are automatically generated when questions contain words like: chart, graph, plot, visualize, trend, distribution, compare 

- The system will analyze your SQL results and create appropriate Chart.js visualizations 

- No need to include chart blocks in your response - they're generated automatically 

- Focus on creating SQL that returns good data for visualization (aggregated, grouped, limited rows) 

  

SIMPLE PATTERN APPROACH: 

For pattern analysis, use straightforward JOINs and aggregation: 

- Join UserActivity with Devices to get device names 

- Filter for failed logins in recent timeframe 

- Count failed logins per device 

- Order by count to find unusual patterns 

For counts: SELECT, COUNT(), GROUP BY, ORDER BY. 

For trends: Filter dates (e.g., DATEADD(day, -90, GETDATE())), GROUP BY DATEPART(). 

For charts: Match SQL output to chart data structure. 

  

EXAMPLE VALID SQL FOR BOOLEAN EFFECTIVENESS: 

```sql 

SELECT  

    d.[DeviceType], 

    COUNT(*) as TotalDevices, 

    SUM(CASE WHEN d.[EndpointProtection] = 'True' THEN 1 ELSE 0 END) as ProtectedDevices, 

    SUM(CASE WHEN d.[EndpointProtection] = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ProtectionEffectiveness, 

    SUM(CASE WHEN d.[IsEncrypted] = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as EncryptionCoverage 

FROM [dbo].[Devices] d 

GROUP BY d.[DeviceType] 

ORDER BY ProtectionEffectiveness DESC 

``` 

 

EXAMPLE FOR LISTING UNPROTECTED DEVICES: 

```sql 

SELECT TOP 50 

    d.[DeviceID], 

    d.[Name] as DeviceName, 

    d.[DeviceType], 

    d.[EndpointProtection], 

    d.[IsEncrypted] 

FROM [dbo].[Devices] d 

WHERE d.[EndpointProtection] = 'False' 

   OR d.[IsEncrypted] = 'False' 

ORDER BY d.[DeviceType], d.[Name] 

``` 

  

WHAT NOT TO WRITE: 

❌ AVG([BooleanColumn]) without CASE conversion 

❌ SUM([BooleanColumn]) without handling True/False strings 

❌ Multiple SELECT statements 

❌ CTEs with WITH clauses 

❌ Incomplete FROM clauses 

❌ Missing table names in FROM 

❌ Complex nested queries 

❌ Non-Chart.js chart formats 

 

  

""" 

 

def format_schema_for_prompt(tables_info: List[Dict]) -> str: 

    return f"AVAILABLE SCHEMA:\n{json.dumps(tables_info, indent=2, default=safe_json_serialize)}" 

 

async def build_chatgpt_system_prompt(question: str, tables_info: List[Dict], conversation_history: List[Dict] = None) -> str: 

    base_prompt = load_base_prompt() 

    schema_section = format_schema_for_prompt(filter_schema_for_question(question, tables_info)) 

    context_section = await add_smart_context_to_prompt("", conversation_history, question) 

     

    # Enhanced question analysis 

    question_analysis = f""" 

QUESTION ANALYSIS: 

User Question: "{question}" 

 

Key Requirements: 

- Generate SQL using ONLY the tables and columns shown in the schema above 

- Focus on answering the specific question asked 

- Use appropriate JOINs to link related data 

- Apply filters and aggregations as needed 

- Ensure all column references are valid 

 

""" 

    return f"{base_prompt}\n\n{schema_section}\n\n{question_analysis}{context_section}\"" 

 

def get_predefined_query(question: str) -> Optional[str]: 

    return None 

 

async def ask_intelligent_llm_async(prompt: str) -> str: 

    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT") 

    if not deployment: 

        raise ValueError("AZURE_OPENAI_DEPLOYMENT not set") 

    try: 

        response = await client.chat.completions.create( 

            model=deployment, 

            messages=[ 

                {"role": "system", "content": "You are a helpful, friendly AI assistant with expertise in data analysis."}, 

                {"role": "user", "content": prompt} 

            ], 

            temperature=0.1, 

            max_tokens=2000, 

            seed=42 

        ) 

        return response.choices[0].message.content 

    except Exception as e: 

        logger.error("LLM request failed", error=str(e)) 

        raise 

 

class IntelligentRequest(BaseModel): 

    question: str 

 

    @validator("question") 

    def validate_question(cls, value): 

        if not value.strip(): 

            raise ValueError("Question cannot be empty") 

        if len(value) < 3: 

            raise ValueError("Question is too short; please provide more details") 

        return value.strip() 

 

async def cached_intelligent_analyze(question: str, session_id: str = None) -> Dict[str, Any]: 

    """Modified - Support dynamic session IDs""" 

     

    actual_session_id = session_id if session_id else "default-session-1234567890" 

     

    # Skip KQL cache lookup for schema queries 

    if question.lower() in ['tables_info', 'schema_info']: 

        return await intelligent_analyze(question, actual_session_id) 

     

    # Check KQL cache for user questions only (with session context) 

    cached_result = await get_from_kql_cache(question, actual_session_id) 

    logger.info("Cache check", question=question, session_id=actual_session_id, cache_hit=bool(cached_result)) 

     

    if cached_result: 

        logger.info("Cache hit", question=question, session_id=actual_session_id) 

        cached_result["session_id"] = actual_session_id 

        return cached_result 

     

    # Process new question 

    result = await intelligent_analyze(question, actual_session_id) 

     

    # Store in KQL with session ID 

    await store_in_kql(question, result, result.get("conversation_history", []), actual_session_id) 

    logger.info("Processed and stored result", question=question, session_id=actual_session_id) 

     

    return result 

@mcp.tool("fabric_intelligent_analyze") 

async def intelligent_analyze(question: str, session_id: str = None) -> Dict[str, Any]: 

    start_total = time.time() 

    actual_session_id = session_id if session_id else "default-session-1234567890" 

    try: 

        history_query = f""" 

        ChatHistory 

        | where SessionID == '{actual_session_id}' 

        | order by Timestamp desc 

        | take 10 

        | project Context 

        """ 

        conversation_history = [] 

        try: 

            result = await asyncio.get_event_loop().run_in_executor( 

                None, lambda: kusto_client.execute(KUSTO_DATABASE, history_query) 

            ) 

            for row in result.primary_results[0]: 

                context = json.loads(row["Context"]) 

                conversation_history.extend(context) 

        except Exception as e: 

            logger.error("Failed to fetch conversation history", error=str(e)) 

 

        vague_questions = ["hi", "hello", "hey", "greetings"] 

        if question.lower().strip() in vague_questions: 

            start_convo = time.time() 

            conversational_prompt = f"""The user said: \"{question}\" 

 

This is a casual greeting. Provide a friendly response that: 

1. Acknowledges their greeting 

2. Explains what this analytics tool can do 

3. Suggests example questions 

4. Invites a specific question""" 

            conversational_response = await ask_intelligent_llm_async(conversational_prompt) 

            logger.info("Conversational LLM call", duration=time.time() - start_convo) 

            response = { 

                "question": question, 

                "response_type": "conversational", 

                "analysis": conversational_response, 

                "timestamp": datetime.now().isoformat(), 

                "session_id": session_id, 

                "conversation_history": conversation_history 

            } 

            return response 

 

        tables_info = await get_cached_tables_info() 

        if not tables_info: 

            response = { 

                "question": question, 

                "error": "No accessible tables found.", 

                "analysis": "The system couldn't find any tables in your database.", 

                "suggestion": "Check database connection and permissions. Try asking about specific data tables.", 

                "session_id": session_id, 

                "conversation_history": conversation_history 

            } 

            return response 

 

        potential_sql = get_predefined_query(question) 

        analysis = "Using predefined query for common question type" 

        if not potential_sql: 

            start_llm = time.time() 

            base_prompt = await build_chatgpt_system_prompt(question, tables_info, conversation_history) 

            logger.info("Sending prompt to LLM", prompt_length=len(base_prompt)) 

            #prompt_with_context = await add_smart_context_to_prompt(base_prompt, conversation_history, question) 

            llm_response = await ask_intelligent_llm_async(base_prompt) 

            logger.info("LLM call", duration=time.time() - start_llm) 

            if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response: 

                print(f"🔍 LLM RESPONSE DEBUG:") 

                print(f"  Full response length: {len(llm_response)}") 

                print(f"  Contains SQL_QUERY: {'SQL_QUERY:' in llm_response}") 

                print(f"  Contains ANALYSIS: {'ANALYSIS:' in llm_response}") 

                print(f"  Response preview: {llm_response[:200]}...") 

                 

                parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1) 

                potential_sql = clean_generated_sql(parts[0].strip()) 

                analysis = parts[1].strip() if len(parts) > 1 else "Analysis not found" 

                 

                print(f"  Extracted SQL length: {len(potential_sql)}") 

                print(f"  Extracted analysis length: {len(analysis)}") 

                print(f"  SQL preview: {potential_sql[:100]}...") 

                print(f"  Analysis preview: {analysis[:100]}...") 

            else: 

                print(f"❌ LLM RESPONSE FORMAT ISSUE:") 

                print(f"  Response length: {len(llm_response)}") 

                print(f"  Response preview: {llm_response[:300]}...") 

                parts = llm_response.split('\n\n', 1) 

                potential_sql = clean_generated_sql(parts[0]) if len(parts) >= 1 else "" 

                analysis = parts[1] if len(parts) > 1 else llm_response 

                 

                print(f"  Fallback SQL: {potential_sql[:100]}...") 

                print(f"  Fallback analysis: {analysis[:100]}...") 

 

        if potential_sql and any(keyword in question.lower() for keyword in ['trend', 'predict', 'likely', 'next month', 'based on past', 'future']): 

            if 'DATEADD' not in potential_sql.upper(): 

                logger.warning("Temporal query missing time filtering", question=question) 

                start_temporal_fix = time.time() 

                temporal_prompt = f"""The previous SQL query for temporal analysis is missing time filtering: 

 

Original Question: "{question}" 

Generated SQL: {potential_sql} 

Available Schema: {json.dumps(tables_info[:3], indent=2, default=safe_json_serialize)} 

 

For predictive analysis: 

1. Filter to recent periods (last 6 months): WHERE [date_column] >= DATEADD(MONTH, -6, GETDATE()) 

2. Use a date column from the schema (e.g., DetectionTime, IncidentDate, or similar) 

3. Compare time periods using GROUP BY DATEPART(MONTH, [date_column]) 

4. Include devices with recent activity 

5. Use JOINs based on foreign keys if needed 

6. Ensure the query uses only SELECT, FROM, WHERE, JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, GROUP BY, ORDER BY, HAVING, AND, OR, ON 

 

Generate an improved SQL query. 

 

Format: 

SQL_QUERY: 

[Improved SQL] 

 

ANALYSIS: 

[Explanation]""" 

                try: 

                    temporal_response = await ask_intelligent_llm_async(temporal_prompt) 

                    if "SQL_QUERY:" in temporal_response: 

                        temporal_parts = temporal_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1) 

                        improved_sql = clean_generated_sql(temporal_parts[0].strip()) 

                        if improved_sql and improved_sql.upper().startswith("SELECT") and "DATEADD" in improved_sql.upper(): 

                            potential_sql = improved_sql 

                            analysis = temporal_parts[1].strip() if len(temporal_parts) > 1 else analysis + " (Query improved)" 

                    logger.info("Temporal query improvement", duration=time.time() - start_temporal_fix) 

                except Exception as e: 

                    logger.warning("Temporal query improvement failed", error=str(e)) 

 

        if potential_sql: 

            try: 

                potential_sql = sanitize_sql(potential_sql) 

                table_pattern = r'\[([^\]]+)\]\.\[([^\]]+)\]' 

                used_tables = re.findall(table_pattern, potential_sql) 

                used_table_names = [f"[{schema}].[{table}]" for schema, table in used_tables] 

                available_tables = [table_info['table'] for table_info in tables_info] 

                invalid_tables = [t for t in used_table_names if not any(t.lower() == at.lower() for at in available_tables)] 

                if invalid_tables: 

                    logger.error("SQL uses non-existent tables", invalid_tables=invalid_tables, available_tables=available_tables) 

                    response = { 

                        "question": question, 

                        "error": "I couldn't find some data tables needed to answer your question.", 

                        "analysis": f"The system tried to use tables {invalid_tables} that don't exist. Available tables: {', '.join(available_tables)}.", 

                        "suggestion": f"Please try asking about data in these tables: {', '.join(available_tables[:5])}. For example, 'Show me data from {available_tables[0]}'.", 

                        "technical_details": f"Generated SQL: {potential_sql}", 

                        "session_id": session_id, 

                        "conversation_history": conversation_history 

                    } 

                    return response 

            except ValueError as e: 

                logger.error("SQL sanitization failed", error=str(e)) 

                response = { 

                    "question": question, 

                    "error": "Generated SQL contains unsupported constructs.", 

                    "analysis": f"The generated SQL was invalid: {str(e)}.", 

                    "suggestion": f"Try rephrasing your question or asking about specific data in these tables: {', '.join([table_info['table'] for table_info in tables_info[:5]])}", 

                    "session_id": session_id, 

                    "conversation_history": conversation_history 

                } 

                return response 

 

        if not potential_sql or not potential_sql.upper().startswith("SELECT"): 

            if any(keyword in question.lower() for keyword in ["show", "which", "what", "find", "analyze", "their", "those"]): 

                start_fallback = time.time() 

                force_sql_prompt = f"""This is a data analysis question requiring SQL. 

 

Question: {question} 

 

Use ONLY these tables: 

{chr(10).join(['- ' + table_info['table'] for table_info in tables_info])} 

 

Available schema: 

{json.dumps(tables_info[:3], indent=2, default=safe_json_serialize)} 

 

Generate a comprehensive SQL query with JOINs. 

 

Format: 

SQL_QUERY: 

[Complete SQL] 

 

ANALYSIS: 

[Explanation]""" 

                force_sql_prompt = await add_smart_context_to_prompt(force_sql_prompt, conversation_history, question) 

                try: 

                    forced_response = await ask_intelligent_llm_async(force_sql_prompt) 

                    if "SQL_QUERY:" in forced_response: 

                        forced_parts = forced_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1) 

                        forced_sql = clean_generated_sql(forced_parts[0].strip()) 

                        if forced_sql and forced_sql.upper().startswith("SELECT") and "FROM" in forced_sql.upper(): 

                            potential_sql = sanitize_sql(forced_sql) 

                            analysis = forced_parts[1].strip() if len(forced_parts) > 1 else "Analysis provided" 

                        else: 

                            raise Exception("Invalid SQL generated") 

                    else: 

                        raise Exception("No SQL generated") 

                    logger.info("Fallback LLM call", duration=time.time() - start_fallback) 

                except Exception as e: 

                    response = { 

                        "question": question, 

                        "error": "Could not generate SQL for this question.", 

                        "analysis": "This appears to be a complex data question. There might be an issue with query complexity or data structure.", 

                        "suggestion": f"Try asking about specific aspects using these tables: {', '.join([table_info['table'] for table_info in tables_info[:5]])}", 

                        "session_id": session_id, 

                        "conversation_history": conversation_history 

                    } 

                    return response 

            else: 

                start_convo = time.time() 

                conversational_prompt = f"""The user asked: "{question}" 

 

This doesn't require database analysis. Provide a conversational response that: 

1. Addresses the question 

2. Explains relevant concepts 

3. Offers data analysis help 

4. Suggests data exploration""" 

                conversational_response = await ask_intelligent_llm_async(conversational_prompt) 

                logger.info("Conversational LLM call", duration=time.time() - start_convo) 

                response = { 

                    "question": question, 

                    "response_type": "conversational", 

                    "analysis": conversational_response, 

                    "timestamp": datetime.now().isoformat(), 

                    "session_id": session_id, 

                    "conversation_history": conversation_history 

                } 

                return response 

 

        loop = asyncio.get_event_loop() 

        start_query = time.time() 

        results = await loop.run_in_executor(None, lambda: execute_query(potential_sql)) 

        logger.info("Query execution", duration=time.time() - start_query) 

 

        query_context = extract_context_from_results(results) 

        response = { 

            "question": question, 

            "generated_sql": potential_sql, 

            "analysis": analysis, 

            "result_count": len(results), 

            "sample_data": results[:5] if results else [], 

            "timestamp": datetime.now().isoformat(), 

            "session_id": session_id, 

            "conversation_history": conversation_history 

        } 

 

        conversation_history.append({"role": "user", "content": question}) 

        conversation_history.append({ 

            "role": "assistant", 

            "content": f"Generated SQL and found {len(results)} results", 

            "sql": potential_sql, 

            "context": query_context 

        }) 

        if len(conversation_history) > 10: 

            conversation_history = conversation_history[-10:] 

 

        background_tasks = BackgroundTasks() 

        await add_visualization_to_response(question, potential_sql, results, response) 

 

        if results: 

            start_enhanced = time.time() 

            query_feedback = "" 

            if any(keyword in question.lower() for keyword in ['trend', 'predict', 'likely', 'next month', 'based on past']): 

                query_feedback = f"\n\n**QUERY ANALYSIS NOTE**: {'Good - this query uses time filtering.' if 'DATEADD' in potential_sql.upper() else 'This query lacks sufficient time filtering for predictive analysis.'}" 

            enhanced_prompt = f""" 

User Question: {question} 

 

Query Results: {len(results)} records 

Generated SQL: {potential_sql} 

{query_feedback} 

 

Sample Data: {json.dumps(results[:10], indent=2, default=safe_json_serialize)} 

 

Provide a conversational response that: 

1. Summarizes results 

2. Explains business context 

3. Identifies key patterns 

4. Provides actionable recommendations 

5. Suggests next steps""" 

            response["analysis"] = await ask_intelligent_llm_async(enhanced_prompt) 

            logger.info("Enhanced analysis", duration=time.time() - start_enhanced) 

        else: 

            start_no_data = time.time() 

            no_data_prompt = f""" 

The query for '{question}' returned no results. 

 

SQL: {potential_sql} 

 

Explain: 

1. Why no data was found 

2. Business context 

3. Alternative analysis approaches 

4. Next steps""" 

            response["analysis"] = await ask_intelligent_llm_async(no_data_prompt) 

            logger.warning("No-data query", duration=time.time() - start_no_data) 

 

        logger.info("Total processing time", duration=time.time() - start_total) 

        response["session_id"] = actual_session_id 

        return response 

 

    except Exception as e: 

        logger.error("Analysis failed", error=str(e)) 

        response = { 

            "question": question, 

            "error": f"Analysis_error: {str(e)}", 

            "analysis": "I encountered an error while analyzing your question.", 

            "suggestion": "Try rephrasing your question, e.g., 'What are the recent incident trends for devices?'", 

            "session_id": actual_session_id, 

            "conversation_history": conversation_history 

        } 

        return response 

     

@app.post("/api/fabric/intelligent") 

@limiter.limit("10/minute") 

async def intelligent_analyze_endpoint( 

    req: IntelligentRequest,  

    background_tasks: BackgroundTasks,  

    request: Request, 

    session: Optional[str] = Query(None, description="Session ID") 

): 

    try: 

        # Use dynamic session ID from UI 

        session_id = get_session_id_from_request(session) 

         

        # Process the question with the correct session ID 

        result = await cached_intelligent_analyze(req.question, session_id) 

         

        if "error" in result and result.get("response_type") != "conversational": 

            raise HTTPException(status_code=400, detail=result["error"]) 

         

        # Ensure session_id is in response 

        result["session_id"] = session_id 

         

        return result 

    except Exception as e: 

        logger.error("Endpoint error", error=str(e)) 

        raise HTTPException(status_code=500, detail=str(e)) 

     

@app.on_event("startup") 

async def startup_event(): 

    """Enhanced startup with schema preloading""" 

    try: 

        logger.info("Starting application initialization...") 

         

        # Test KQL connection first 

        kql_ok = await test_kql_connection() 

        if not kql_ok: 

            logger.warning("KQL connection failed during startup") 

        else: 

            logger.info("KQL connection test passed") 

             

        # Initialize KQL table 

        await initialize_kql_table() 

         

        # Preload schema - this is the key addition 

        schema_preloaded = await preload_schema() 

        if schema_preloaded: 

            print("✅ Schema preloaded - first query will be fast!") 

        else: 

            print("⚠️  Schema preload failed - first query may be slower") 

         

        logger.info("Application startup completed successfully") 

         

    except Exception as e: 

        logger.error("Startup failed", error=str(e)) 

        print(f"❌ Startup Error: {e}") 

 

# Add endpoint to manually refresh schema cache 

@app.post("/api/schema/refresh") 

async def refresh_schema_cache(): 

    """Manually refresh the schema cache""" 

    global CACHED_TABLES_INFO, SCHEMA_CACHE_TIMESTAMP 

     

    try: 

        logger.info("Manual schema refresh requested") 

         

        # Clear current cache 

        CACHED_TABLES_INFO = None 

        SCHEMA_CACHE_TIMESTAMP = None 

         

        # Fetch fresh schema 

        tables_info = await get_cached_tables_info() 

         

        return { 

            "status": "success", 

            "message": "Schema cache refreshed successfully", 

            "table_count": len(tables_info), 

            "timestamp": datetime.now().isoformat() 

        } 

         

    except Exception as e: 

        logger.error("Schema refresh failed", error=str(e)) 

        raise HTTPException(status_code=500, detail=f"Schema refresh failed: {str(e)}") 

             

 

@app.get("/api/chat/messages") 

async def get_chat_messages( 

    session: Optional[str] = Query(None, description="Session ID"), 

    limit: Optional[int] = Query(10, description="Number of recent conversations to return") 

): 

    """Get chat messages for specified session with session validation""" 

     

    session_id = get_session_id_from_request(session) 

     

    try: 

        # First check if this session exists 

        check_query = f""" 

        ChatHistory 

        | where SessionID == '{session_id}' 

        | count 

        """ 

         

        check_result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, check_query) 

        ) 

         

        session_exists = check_result.primary_results[0][0]["Count"] > 0 if check_result.primary_results[0] else False 

         

        history_query = f""" 

        ChatHistory 

        | where SessionID == '{session_id}' 

        | where Question != 'tables_info' and Question != 'schema_info' 

        | order by Timestamp desc 

        | take {limit * 2} 

        | order by Timestamp asc 

        | project Timestamp, Question, Response 

        """ 

          

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, history_query) 

        ) 

         

        messages = [] 

        for row in result.primary_results[0]: 

            try: 

                response_data = json.loads(row["Response"]) 

                 

                messages.append({ 

                    "id": f"user_{len(messages)}", 

                    "type": "user", 

                    "content": row["Question"], 

                    "timestamp": row["Timestamp"] 

                }) 

                 

                messages.append({ 

                    "id": f"assistant_{len(messages)}", 

                    "type": "assistant",  

                    "content": response_data.get("analysis", "No analysis available"), 

                    "sql": response_data.get("generated_sql"), 

                    "result_count": response_data.get("result_count", 0), 

                    "sample_data": response_data.get("sample_data", []), 

                    "visualization": response_data.get("visualization"), 

                    "timestamp": row["Timestamp"] 

                }) 

                 

            except json.JSONDecodeError: 

                continue 

             

        return { 

            "status": "success", 

            "session_id": session_id, 

            "session_exists": session_exists, 

            "messages": messages, 

            "message_count": len(messages), 

            "total_pairs": len(messages) // 2 

        } 

         

    except Exception as e: 

        logger.error("Failed to retrieve chat messages", session_id=session_id, error=str(e)) 

        return { 

            "status": "error", 

            "session_id": session_id, 

            "session_exists": False, 

            "messages": [], 

            "message_count": 0, 

            "total_pairs": 0, 

            "error": str(e) 

        } 

         

@app.post("/api/chat/clear") 

async def clear_chat_and_start_new_session( 

    session: Optional[str] = Query(None, description="Current Session ID"), 

    create_new: Optional[bool] = Query(True, description="Create new session after clear") 

): 

    """Clear current session and optionally start a new one""" 

     

    current_session_id = get_session_id_from_request(session) 

     

    try: 

        logger.info("Chat clear requested", session_id=current_session_id, create_new=create_new) 

         

        if create_new: 

            # Generate a new session ID 

            new_session_id = generate_new_session_id() 

             

            return { 

                "status": "success", 

                "message": "Chat cleared and new session started", 

                "old_session_id": current_session_id, 

                "new_session_id": new_session_id, 

                "timestamp": datetime.now().isoformat(), 

                "action": "new_session_created" 

            } 

        else: 

            return { 

                "status": "success",  

                "message": "Chat cleared successfully", 

                "session_id": current_session_id, 

                "timestamp": datetime.now().isoformat(), 

                "action": "session_cleared" 

            } 

         

    except Exception as e: 

        logger.error("Chat clear request failed", session_id=current_session_id, error=str(e)) 

        raise HTTPException(status_code=500, detail="Failed to clear chat") 

     

@app.get("/api/chat/sessions") 

async def get_chat_sessions( 

    date: Optional[str] = Query(None, description="Date in YYYYMMDD format, or 'all' for all sessions"), 

    limit: Optional[int] = Query(50, description="Maximum number of sessions to return") 

): 

    """Get chat sessions with dynamic naming and full history support""" 

     

    try: 

        print(f"🔍 DEBUG: Sessions endpoint called with date='{date}', limit={limit}") 

         

        if not date: 

            date = "all" 

             

        if date == "all": 

            sessions_query = f""" 

            ChatHistory 

            | where SessionID startswith 'powerbi_' 

            | where Question != 'tables_info' and Question != 'schema_info' 

            | summarize  

                MessageCount = count(), 

                FirstMessage = min(Timestamp), 

                LastMessage = max(Timestamp), 

                FirstQuestion = take_any(Question), 

                LastQuestion = arg_max(Timestamp, Question) 

            by SessionID 

            | order by LastMessage desc 

            | take {limit} 

            """ 

        else: 

            sessions_query = f""" 

            ChatHistory 

            | where SessionID contains 'powerbi_{date}' 

            | where Question != 'tables_info' and Question != 'schema_info' 

            | summarize  

                MessageCount = count(), 

                FirstMessage = min(Timestamp), 

                LastMessage = max(Timestamp), 

                FirstQuestion = take_any(Question), 

                LastQuestion = arg_max(Timestamp, Question) 

            by SessionID 

            | order by LastMessage desc 

            | take {limit} 

            """ 

         

        print(f"🔍 DEBUG: Executing query...") 

         

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, sessions_query) 

        ) 

         

        raw_results = result.primary_results[0] if result.primary_results else [] 

        print(f"🔍 DEBUG: KQL returned {len(raw_results)} raw results") 

         

        sessions = [] 

        for i, row in enumerate(raw_results): 

            try: 

                # FIXED: Use dictionary-style access instead of .get() 

                session_id = row["SessionID"] 

                message_count = row["MessageCount"] 

                first_message = row["FirstMessage"] 

                last_message = row["LastMessage"] 

                first_question = row["FirstQuestion"] 

                last_question = row.get("LastQuestion", first_question) if hasattr(row, 'get') else row["LastQuestion"] if "LastQuestion" in row else first_question 

                 

                print(f"🔍 DEBUG: Processing row {i+1}: {session_id} ({message_count} messages)") 

                 

                # Use last question for better identification, fallback to first question 

                display_question = last_question or first_question or "Unknown" 

                 

                # Clean and truncate the question for display 

                display_question = str(display_question).strip() 

                if len(display_question) > 45: 

                    display_question = display_question[:45] + "..." 

                 

                # Extract date from session ID for grouping 

                session_parts = session_id.split('_') 

                session_date = "Unknown" 

                 

                if len(session_parts) >= 2: 

                    date_part = session_parts[1] 

                    if len(date_part) == 8:  # YYYYMMDD format 

                        try: 

                            parsed_date = datetime.strptime(date_part, "%Y%m%d") 

                            session_date = parsed_date.strftime("%b %d, %Y") 

                        except: 

                            session_date = date_part 

                 

                session_info = { 

                    "session_id": session_id, 

                    "display_name": display_question, 

                    "message_count": message_count, 

                    "first_message": first_message, 

                    "last_message": last_message, 

                    "first_question": first_question, 

                    "last_question": last_question, 

                    "session_date": session_date, 

                    "is_today": session_date == datetime.now().strftime("%b %d, %Y") 

                } 

                sessions.append(session_info) 

                 

            except Exception as e: 

                print(f"❌ ERROR processing row {i+1}: {e}") 

                print(f"❌ Row data: {row}") 

                continue 

         

        print(f"🔍 DEBUG: Successfully processed {len(sessions)} sessions") 

         

        return { 

            "status": "success", 

            "query_type": "all" if date == "all" else f"date_{date}", 

            "sessions": sessions, 

            "total_sessions": len(sessions) 

        } 

         

    except Exception as e: 

        print(f"❌ ERROR in get_chat_sessions: {str(e)}") 

        import traceback 

        print(f"❌ TRACEBACK: {traceback.format_exc()}") 

        logger.error("Failed to retrieve sessions", date=date, error=str(e)) 

        return { 

            "status": "error", 

            "query_type": "all" if date == "all" else f"date_{date}", 

            "sessions": [], 

            "total_sessions": 0, 

            "error": str(e) 

        } 

                 

# Remove or modify the existing cache clear endpoint to make it admin-only 

@app.delete("/api/admin/cache/clear") 

async def admin_clear_kql_cache(): 

    """ADMIN ONLY: Clear the entire KQL ChatHistory table""" 

    try: 

        clear_query = ".drop table ChatHistory" 

        await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, clear_query) 

        ) 

        await initialize_kql_table() 

         

        logger.warning("ADMIN: KQL cache cleared completely") 

         

        return { 

            "status": "success", 

            "message": "KQL cache cleared completely - ALL conversation history deleted", 

            "timestamp": datetime.now().isoformat(), 

            "warning": "This action cannot be undone" 

        } 

    except Exception as e: 

        logger.error("Admin KQL cache clear failed", error=str(e)) 

        raise HTTPException(status_code=500, detail=f"Failed to clear KQL cache: {str(e)}") 

 

# Enhanced health check to show chat status 

@app.get("/health") 

async def health_check(): 

    """Enhanced health check with chat session info""" 

    global CACHED_TABLES_INFO, SCHEMA_CACHE_TIMESTAMP 

     

    health_status = { 

        "status": "healthy", 

        "timestamp": datetime.now().isoformat(), 

        "services": {}, 

        "schema_cache": {}, 

        "chat_session": { 

            "session_id": "default-session-1234567890", 

            "ready": True 

        } 

    } 

     

    try: 

        # Test SQL Database 

        loop = asyncio.get_event_loop() 

        await loop.run_in_executor(None, lambda: execute_query("SELECT 1")) 

        health_status["services"]["sql_database"] = "connected" 

    except Exception as e: 

        health_status["services"]["sql_database"] = f"error: {str(e)}" 

        health_status["status"] = "degraded" 

     

    try: 

        # Test KQL Database 

        test_result = await test_kql_connection() 

        health_status["services"]["kql_database"] = "connected" if test_result else "error" 

        if not test_result: 

            health_status["status"] = "degraded" 

    except Exception as e: 

        health_status["services"]["kql_database"] = f"error: {str(e)}" 

        health_status["status"] = "degraded" 

     

    # Schema cache status 

    if CACHED_TABLES_INFO is not None: 

        cache_age = time.time() - (SCHEMA_CACHE_TIMESTAMP or 0) 

        health_status["schema_cache"] = { 

            "status": "loaded", 

            "table_count": len(CACHED_TABLES_INFO), 

            "cache_age_seconds": int(cache_age), 

            "is_fresh": cache_age < SCHEMA_CACHE_DURATION 

        } 

    else: 

        health_status["schema_cache"] = { 

            "status": "not_loaded", 

            "message": "Schema not cached yet" 

        } 

     

    # Quick chat history count 

    try: 

        count_query = f""" 

        ChatHistory 

        | where SessionID == 'default-session-1234567890' 

        | where Question != 'tables_info' and Question != 'schema_info' 

        | count 

        """ 

        result = await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, count_query) 

        ) 

         

        if result.primary_results and len(result.primary_results[0]) > 0: 

            chat_count = result.primary_results[0][0]["Count"] 

            health_status["chat_session"]["stored_conversations"] = chat_count 

    except: 

        health_status["chat_session"]["stored_conversations"] = "unknown" 

     

    health_status["features"] = [ 

        "Natural language processing",  

        "SQL analytics",  

        "Business insights",  

        "Smart visualization",  

        "KQL storage", 

        "In-memory schema caching", 

        "UI chat management" 

    ] 

     

    if health_status["status"] == "degraded": 

        raise HTTPException(status_code=503, detail=health_status) 

         

    return health_status 

 

@app.get("/api/fabric/capabilities") 

def get_capabilities(): 

    return { 

        "capabilities": "Natural language query analysis with KQL persistence", 

        "example_questions": [ 

            "What is the average cyber risk score?", 

            "Show critical vulnerabilities (CVSS >= 7.0)", 

            "Count unpatched devices by type", 

            "Show login failure trends over time", 

            "What are their departments?" 

        ], 

        "calculation_features": [ 

            "SQL-based stats", 

            "Aggregations and percentages", 

            "Dynamic risk scores", 

            "Trend analysis", 

            "Group-based comparisons", 

            "Real-time metrics" 

        ], 

        "intelligence_features": [ 

            "Natural language understanding", 

            "Context-aware answers", 

            "Proactive suggestions", 

            "Detailed explanations", 

            "Business insights" 

        ], 

        "visualization_features": [ 

            "Smart chart", 

            "Pillar charts for comparisons", 

            "Pillar charts for trends", 

            "Pie charts for distributions", 

            "Stacked bars for grouped data" 

        ], 

        "supported_analysis": [ 

            "Cybersecurity risks", 

            "Vulnerability tracking", 

            "Patch monitoring", 

            "Compliance checks", 

            "Performance metrics", 

            "Trends analysis", 

            "Comparative studies", 

            "Predictive insights" 

        ] 

    } 

 

@app.get("/api/fabric/history") 

async def get_history(session: Optional[str] = Query(None, description="Session ID")): 

    """ 

    Retrieve the latest 10 conversation responses for the fixed session ID. 

    """ 

    try: 

        responses = await get_latest_responses() 

        return { 

            "status": "success", 

            "session_id": "default-session-1234567890", 

            "history": responses 

        } 

    except Exception as e: 

        logger.error("Failed to retrieve history", error=str(e)) 

        raise HTTPException(status_code=500, detail="Failed to retrieve conversation history") 

 

@app.delete("/api/cache/clear") 

async def clear_kql_cache(): 

    """ 

    Clear the KQL ChatHistory table. 

    """ 

    try: 

        clear_query = """ 

        .drop table ChatHistory 

        """ 

        await asyncio.get_event_loop().run_in_executor( 

            None, lambda: kusto_client.execute(KUSTO_DATABASE, clear_query) 

        ) 

        await initialize_kql_table() 

        return { 

            "status": "success", 

            "message": "KQL cache cleared successfully", 

            "timestamp": datetime.now().isoformat() 

        } 

    except Exception as e: 

        logger.error("KQL cache clear failed", error=str(e)) 

        raise HTTPException(status_code=500, detail=f"Failed to clear KQL cache: {str(e)}") 

 

def validate_environment(): 

    required_vars = [ 

        "AZURE_OPENAI_API_KEY", 

        "AZURE_OPENAI_ENDPOINT", 

        "AZURE_OPENAI_DEPLOYMENT", 

        "FABRIC_SQL_ENDPOINT", 

        "FABRIC_DATABASE", 

        "FABRIC_CLIENT_ID", 

        "FABRIC_CLIENT_SECRET", 

        "KUSTO_CLUSTER", 

        "KUSTO_DATABASE", 

        "FABRIC_TENANT_ID" 

    ] 

    missing = [var for var in required_vars if not os.getenv(var)] 

    if missing: 

        logger.error("Missing environment variables", missing=missing) 

        raise RuntimeError(f"Missing required environment variables: {missing}") 

 

async def startup_event(): 

    await initialize_kql_table() 

 

app.add_event_handler("startup", startup_event) 

 

if __name__ == "__main__": 

    print("🤖 Intelligent SQL Analytics Assistant") 

    print("📊 Powered by Microsoft Fabric SQL Database and KQL Storage") 

    print("🖥 Advanced analytics engine") 

    print("📈 Smart visualization") 

    print("") 

    print("✨ Key Features:") 

    print("• Natural language queries") 

    print("• Automatic SQL generation") 

    print("• Business-oriented insights") 

    print("• Context-aware visualizations") 

    print("• KQL-based conversation history") 

    print("") 

    print("💡 Example Questions:") 

    print("• 'What is the average cyber risk score?'") 

    print("• 'Show critical vulnerabilities (CVSS ≥ 7.0)'") 

    print("• 'How many unpatched devices by type?'") 

    print("• 'Show trends in incidents over time'") 

    print("• 'What are their departments?'") 

    print("") 

    print("🔗 API: POST /api/fabric/intelligent") 

    print("📜 History: GET /api/fabric/history") 

    print("🏥 Health: GET /health") 

    print("📘 Capabilities: GET /api/fabric/capabilities") 

    validate_environment() 

    uvicorn.run("__main__:app", host="0.0.0.0", port=8005) 
