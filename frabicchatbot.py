#!/usr/bin/env python3
"""
Fabric Lakehouse Analytics Chatbot - Python Flask Application
A web-based chatbot for analyzing Fabric Lakehouse data using natural language.
"""

from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import sqlite3
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

class FabricLakehouseConnector:
    """Mock connector for Fabric Lakehouse - replace with actual implementation"""
    
    def __init__(self):
        self.connection_string = None
        self.is_connected = False
        self.available_tables = []
        self._setup_mock_data()
    
    def _setup_mock_data(self):
        """Setup mock data for demonstration"""
        self.available_tables = [
            {"full_name": "sales.customers", "schema": "sales", "table": "customers"},
            {"full_name": "sales.orders", "schema": "sales", "table": "orders"},
            {"full_name": "inventory.products", "schema": "inventory", "table": "products"},
            {"full_name": "finance.transactions", "schema": "finance", "table": "transactions"},
            {"full_name": "hr.employees", "schema": "hr", "table": "employees"}
        ]
    
    def test_connection(self) -> bool:
        """Test connection to Fabric Lakehouse"""
        try:
            # Mock connection test - replace with actual Fabric connection logic
            self.is_connected = True
            logger.info("Connection to Fabric Lakehouse successful")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            self.is_connected = False
            return False
    
    def get_tables(self) -> List[Dict[str, str]]:
        """Get available tables from Fabric Lakehouse"""
        if not self.is_connected:
            return []
        return self.available_tables
    
    def execute_query(self, table: str, question: str, limit: int = 10) -> str:
        """Execute analysis query - replace with actual Fabric query logic"""
        try:
            # Mock analysis response - replace with actual AI/ML analysis
            mock_responses = {
                "sales.customers": f"Analysis of {table}: Found {limit} customer records. Top customers by revenue show strong growth in Q4.",
                "sales.orders": f"Analysis of {table}: Analyzing {limit} order records. Average order value increased 15% this quarter.",
                "inventory.products": f"Analysis of {table}: Product analysis shows {limit} items. Best selling category is electronics.",
                "finance.transactions": f"Analysis of {table}: Financial data shows {limit} transactions with positive cash flow trends.",
                "hr.employees": f"Analysis of {table}: Employee data analysis of {limit} records shows 95% retention rate."
            }
            
            response = mock_responses.get(table, f"Analysis complete for {table} with {limit} records.")
            response += f"\n\nQuery: '{question}'\nRows analyzed: {limit}"
            
            return response
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return f"Error executing query: {str(e)}"

# Initialize the connector
fabric_connector = FabricLakehouseConnector()

@app.route('/')
def index():
    """Serve the main chatbot interface"""
    return render_template('index.html')

@app.route('/api/fabric/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        is_healthy = fabric_connector.test_connection()
        return jsonify({
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "connected": fabric_connector.is_connected
        }), 200 if is_healthy else 503
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/fabric/tables', methods=['GET'])
def get_tables():
    """Get available tables from Fabric Lakehouse"""
    try:
        tables = fabric_connector.get_tables()
        return jsonify({
            "tables": tables,
            "count": len(tables),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Failed to get tables: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/fabric/analyze', methods=['POST'])
def analyze_data():
    """Analyze data based on natural language question"""
    try:
        data = request.get_json()
        
        # Validate request
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        question = data.get('question', '').strip()
        table = data.get('table', '').strip()
        limit = data.get('limit', 10)
        
        if not question:
            return jsonify({"error": "Question is required"}), 400
        
        if not table:
            return jsonify({"error": "Table selection is required"}), 400
        
        # Validate limit
        try:
            limit = int(limit)
            if limit < 1:
                limit = 10
            elif limit > 1000000:
                limit = 1000000
        except (ValueError, TypeError):
            limit = 10
        
        # Execute analysis
        analysis_result = fabric_connector.execute_query(table, question, limit)
        
        return jsonify({
            "analysis": analysis_result,
            "question": question,
            "table": table,
            "limit": limit,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        return jsonify({"error": f"Analysis failed: {str(e)}"}), 500

@app.route('/api/fabric/disconnect', methods=['POST'])
def disconnect():
    """Disconnect from Fabric Lakehouse"""
    try:
        fabric_connector.is_connected = False
        fabric_connector.available_tables = []
        return jsonify({
            "status": "disconnected",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Disconnect failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    import traceback
    error_details = {
        "error": "Internal server error",
        "message": str(error),
        "type": type(error).__name__,
        "timestamp": datetime.now().isoformat()
    }
    
    # Log the full error with traceback
    logger.error("Internal server error occurred:", exc_info=True)
    logger.error(f"Error details: {error_details}")
    
    # In development mode, include more details
    if app.debug:
        error_details["traceback"] = traceback.format_exc()
    
    return jsonify(error_details), 500

@app.errorhandler(Exception)
def handle_exception(error):
    """Handle all unhandled exceptions"""
    import traceback
    error_details = {
        "error": "An unexpected error occurred",
        "message": str(error),
        "type": type(error).__name__,
        "timestamp": datetime.now().isoformat()
    }
    
    # Log the full error with traceback
    logger.error("Unhandled exception occurred:", exc_info=True)
    logger.error(f"Error details: {error_details}")
    
    # In development mode, include more details
    if app.debug:
        error_details["traceback"] = traceback.format_exc()
    
    return jsonify(error_details), 500

def create_templates_dir():
    """Create templates directory and HTML file"""
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)
    
    html_content = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fabric Lakehouse Analytics Chatbot</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
 
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            background: #f5f7fa;
            height: 100vh;
            display: flex;
        }
 
        .sidebar {
            width: 280px;
            background: white;
            border-right: 1px solid #e5e7eb;
            display: flex;
            flex-direction: column;
            box-shadow: 2px 0 8px rgba(0, 0, 0, 0.1);
        }
 
        .sidebar-section {
            padding: 16px 20px;
            border-bottom: 1px solid #f1f3f4;
        }
 
        .sidebar-section h3 {
            font-size: 14px;
            font-weight: 600;
            color: #374151;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
 
        .status-indicator {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #ef4444;
        }
 
        .status-indicator.online {
            background: #10b981;
        }
 
        .config-input {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 13px;
            margin-bottom: 8px;
        }
 
        .btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 13px;
            cursor: pointer;
            margin-right: 8px;
            margin-bottom: 8px;
        }
 
        .btn:hover {
            background: #2563eb;
        }
 
        .btn-secondary {
            background: #6b7280;
        }
 
        .table-list {
            max-height: 200px;
            overflow-y: auto;
        }
 
        .table-item {
            padding: 8px 12px;
            margin: 4px 0;
            background: #f8fafc;
            border: 1px solid #e5e7eb;
            border-radius: 4px;
            font-size: 13px;
            color: #374151;
            cursor: pointer;
            transition: all 0.2s;
        }
 
        .table-item:hover {
            background: #e0f2fe;
            border-color: #0ea5e9;
        }
 
        .table-item.selected {
            background: #dbeafe;
            border-color: #3b82f6;
            color: #1d4ed8;
        }
 
        .data-mode-options {
            margin-bottom: 12px;
        }
 
        .radio-option {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 6px;
            font-size: 13px;
            cursor: pointer;
        }
 
        .radio-option input {
            margin: 0;
        }
 
        .slider-container {
            margin: 12px 0;
        }
 
        .slider {
            width: 100%;
            height: 4px;
            border-radius: 2px;
            background: #e5e7eb;
            outline: none;
            -webkit-appearance: none;
            appearance: none;
        }
 
        .slider::-webkit-slider-thumb {
            -webkit-appearance: none;
            appearance: none;
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background: #3b82f6;
            cursor: pointer;
        }
 
        .current-settings {
            background: #f3f4f6;
            padding: 8px;
            border-radius: 4px;
            margin-bottom: 12px;
            font-size: 12px;
            color: #374151;
        }
 
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: white;
        }
 
        .header {
            padding: 16px 24px;
            background: white;
            border-bottom: 1px solid #e5e7eb;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
 
        .header h1 {
            font-size: 24px;
            font-weight: 700;
            color: #111827;
            display: flex;
            align-items: center;
            gap: 12px;
        }
 
        .header p {
            color: #6b7280;
            font-size: 14px;
        }
 
        .status-badge {
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }
 
        .status-badge.connecting {
            background: #fef3c7;
            color: #92400e;
        }
 
        .status-badge.connected {
            background: #d1fae5;
            color: #065f46;
        }
 
        .status-badge.offline {
            background: #fee2e2;
            color: #dc2626;
        }
 
        .chat-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 20px 24px;
        }
 
        .chat-content {
            flex: 1;
            background: #f8fafc;
            border-radius: 8px;
            border: 1px solid #e5e7eb;
            display: flex;
            flex-direction: column;
            margin-bottom: 16px;
        }
 
        .messages {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 16px;
        }
 
        .message {
            max-width: 80%;
            padding: 12px 16px;
            border-radius: 12px;
            font-size: 14px;
            line-height: 1.5;
        }
 
        .message.user {
            background: #3b82f6;
            color: white;
            align-self: flex-end;
            margin-left: auto;
        }
 
        .message.assistant {
            background: #f3f4f6;
            color: #374151;
            align-self: flex-start;
            border: 1px solid #e5e7eb;
            white-space: pre-wrap;
        }
 
        .message.system {
            background: #ecfdf5;
            color: #065f46;
            align-self: center;
            border: 1px solid #a7f3d0;
            max-width: 90%;
            text-align: center;
            font-style: italic;
        }
 
        .input-area {
            padding: 16px 20px;
            background: white;
            border-top: 1px solid #e5e7eb;
            border-radius: 0 0 8px 8px;
            display: flex;
            gap: 12px;
            align-items: flex-end;
        }
 
        .input-container {
            flex: 1;
        }
 
        #messageInput {
            width: 100%;
            padding: 12px 16px;
            border: 1px solid #d1d5db;
            border-radius: 8px;
            font-size: 14px;
            resize: none;
            outline: none;
            font-family: inherit;
            max-height: 120px;
        }
 
        #messageInput:focus {
            border-color: #3b82f6;
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        }
 
        #sendBtn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 12px 20px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }
 
        #sendBtn:hover:not(:disabled) {
            background: #2563eb;
        }
 
        #sendBtn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
 
        .typing-indicator {
            display: none;
            padding: 12px 16px;
            background: #f3f4f6;
            border-radius: 12px;
            align-self: flex-start;
            max-width: 80px;
            border: 1px solid #e5e7eb;
            margin: 0 20px;
        }
 
        .typing-dots {
            display: flex;
            gap: 4px;
            justify-content: center;
        }
 
        .typing-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #9ca3af;
            animation: typingBounce 1.4s ease-in-out infinite both;
        }
 
        .typing-dot:nth-child(1) { animation-delay: -0.32s; }
        .typing-dot:nth-child(2) { animation-delay: -0.16s; }
 
        @keyframes typingBounce {
            0%, 80%, 100% {
                transform: scale(0.8);
                opacity: 0.5;
            }
            40% {
                transform: scale(1);
                opacity: 1;
            }
        }
    </style>
</head>
<body>
    <div class="sidebar">
        <div class="sidebar-section">
            <h3>🔧 Configuration</h3>
            <div style="margin-bottom: 12px;">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                    <div class="status-indicator" id="statusIndicator"></div>
                    <span style="font-size: 13px; color: #374151;" id="statusText">Server is offline</span>
                </div>
            </div>
            <input type="text" id="serverUrl" class="config-input" placeholder="Server URL" value="http://localhost:5000">
            <button class="btn" id="connectBtn">Connect</button>
            <button class="btn btn-secondary" id="refreshBtn">Refresh Tables</button>
        </div>
 
        <div class="sidebar-section">
            <h3>📊 Available Tables</h3>
            <div class="table-list" id="tableList">
                <div style="text-align: center; color: #6b7280; font-size: 13px; padding: 20px;">
                    Connect to server to load tables
                </div>
            </div>
        </div>
 
        <div class="sidebar-section">
            <h3>⚙️ Query Settings</h3>
            <div class="data-mode-options">
                <label class="radio-option">
                    <input type="radio" name="dataMode" value="limited" checked>
                    <span>Limited rows (faster)</span>
                </label>
                <label class="radio-option">
                    <input type="radio" name="dataMode" value="manual">
                    <span>Manual row count</span>
                </label>
                <label class="radio-option">
                    <input type="radio" name="dataMode" value="all">
                    <span>Entire dataset (slow)</span>
                </label>
            </div>
           
            <div id="sliderContainer" class="slider-container">
                <input type="range" id="rowLimit" class="slider" min="5" max="100" value="10">
                <div style="display: flex; justify-content: space-between; font-size: 11px; color: #9ca3af; margin-top: 4px;">
                    <span>5</span>
                    <span id="rowLimitValue">10</span>
                    <span>100</span>
                </div>
            </div>
           
            <div id="manualContainer" style="display: none;">
                <input type="number" id="manualRowCount" class="config-input" placeholder="Enter row count" min="1" max="1000000" value="50">
            </div>
           
            <div class="current-settings">
                <strong>Current:</strong> <span id="settingsDisplay">Analyze 10 rows</span>
            </div>
           
            <button class="btn" id="clearBtn" style="width: 100%;">Clear Chat</button>
        </div>
    </div>
 
    <div class="main-content">
        <div class="header">
            <div>
                <h1>🤖 Fabric Lakehouse Analytics Chatbot</h1>
                <p>Ask questions about your Fabric Lakehouse data using natural language!</p>
            </div>
            <div>
                <span class="status-badge offline" id="connectionBadge">❌ OFFLINE</span>
            </div>
        </div>
 
        <div class="chat-container">
            <div class="chat-content">
                <div class="messages" id="messages">
                    <div class="message system">
                        Welcome! Connect to your server and start asking questions about your data.
                    </div>
                </div>
 
                <div class="typing-indicator" id="typingIndicator">
                    <div class="typing-dots">
                        <div class="typing-dot"></div>
                        <div class="typing-dot"></div>
                        <div class="typing-dot"></div>
                    </div>
                </div>
 
                <div class="input-area">
                    <div class="input-container">
                        <textarea
                            id="messageInput"
                            placeholder="Select a table first, then ask questions..."
                            rows="1"
                        ></textarea>
                    </div>
                    <button id="sendBtn">Send ➤</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        class DataChatbot {
            constructor() {
                this.init();
            }
           
            init() {
                this.elements = {
                    messages: document.getElementById('messages'),
                    messageInput: document.getElementById('messageInput'),
                    sendBtn: document.getElementById('sendBtn'),
                    typingIndicator: document.getElementById('typingIndicator'),
                    serverUrl: document.getElementById('serverUrl'),
                    tableList: document.getElementById('tableList'),
                    statusIndicator: document.getElementById('statusIndicator'),
                    statusText: document.getElementById('statusText'),
                    connectionBadge: document.getElementById('connectionBadge'),
                    rowLimit: document.getElementById('rowLimit'),
                    rowLimitValue: document.getElementById('rowLimitValue'),
                    manualRowCount: document.getElementById('manualRowCount'),
                    settingsDisplay: document.getElementById('settingsDisplay'),
                    connectBtn: document.getElementById('connectBtn'),
                    refreshBtn: document.getElementById('refreshBtn'),
                    clearBtn: document.getElementById('clearBtn')
                };
               
                this.state = {
                    isConnected: false,
                    availableTables: [],
                    selectedTable: null
                };
               
                this.setupEventListeners();
                this.updateSettingsDisplay();
            }
           
            setupEventListeners() {
                this.elements.connectBtn.onclick = () => this.testConnection();
                this.elements.refreshBtn.onclick = () => this.refreshTables();
                this.elements.clearBtn.onclick = () => this.clearChat();
                this.elements.sendBtn.onclick = () => this.sendMessage();
               
                this.elements.messageInput.onkeypress = (e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault();
                        this.sendMessage();
                    }
                };
               
                this.elements.messageInput.oninput = () => this.autoResizeTextarea();
                this.elements.rowLimit.oninput = () => this.updateRowLimitDisplay();
                this.elements.manualRowCount.oninput = () => this.updateSettingsDisplay();
               
                document.querySelectorAll('input[name="dataMode"]').forEach(radio => {
                    radio.onchange = () => this.updateDataMode();
                });
            }
           
            autoResizeTextarea() {
                const input = this.elements.messageInput;
                input.style.height = 'auto';
                input.style.height = Math.min(input.scrollHeight, 120) + 'px';
            }
           
            updateRowLimitDisplay() {
                this.elements.rowLimitValue.textContent = this.elements.rowLimit.value;
                this.updateSettingsDisplay();
            }
           
            updateSettingsDisplay() {
                const dataMode = document.querySelector('input[name="dataMode"]:checked').value;
                let displayText = '';
               
                switch(dataMode) {
                    case 'limited':
                        displayText = `Analyze ${this.elements.rowLimit.value} rows`;
                        break;
                    case 'manual':
                        const manualCount = this.elements.manualRowCount.value || '50';
                        displayText = `Analyze ${manualCount} rows`;
                        break;
                    case 'all':
                        displayText = 'Analyze entire dataset';
                        break;
                }
               
                this.elements.settingsDisplay.textContent = displayText;
            }
           
            updateDataMode() {
                const dataMode = document.querySelector('input[name="dataMode"]:checked').value;
                const sliderContainer = document.getElementById('sliderContainer');
                const manualContainer = document.getElementById('manualContainer');
               
                if (dataMode === 'limited') {
                    sliderContainer.style.display = 'block';
                    manualContainer.style.display = 'none';
                } else if (dataMode === 'manual') {
                    sliderContainer.style.display = 'none';
                    manualContainer.style.display = 'block';
                } else {
                    sliderContainer.style.display = 'none';
                    manualContainer.style.display = 'none';
                }
               
                this.updateSettingsDisplay();
            }
           
            getCurrentRowLimit() {
                const dataMode = document.querySelector('input[name="dataMode"]:checked').value;
               
                switch(dataMode) {
                    case 'limited':
                        return parseInt(this.elements.rowLimit.value);
                    case 'manual':
                        return parseInt(this.elements.manualRowCount.value) || 50;
                    case 'all':
                        return 999999;
                    default:
                        return 10;
                }
            }
           
            async testConnection() {
                const serverUrl = this.elements.serverUrl.value.trim();
                if (!serverUrl) {
                    this.addMessage('Please enter a server URL first', 'system');
                    return;
                }
               
                this.updateConnectionStatus('connecting');
                this.addMessage(`🔍 Testing connection to: ${serverUrl}`, 'system');
               
                try {
                    const response = await fetch(`${serverUrl}/api/fabric/health`, {
                        method: 'GET',
                        headers: { 'Accept': 'application/json' }
                    });
                   
                    if (response.ok) {
                        this.state.isConnected = true;
                        this.updateConnectionStatus('connected');
                        this.addMessage('✅ Successfully connected!', 'system');
                        await this.loadTables();
                    } else {
                        throw new Error(`Server responded with ${response.status}`);
                    }
                   
                } catch (error) {
                    this.state.isConnected = false;
                    this.updateConnectionStatus('offline');
                    this.addMessage(`❌ Connection failed: ${error.message}`, 'system');
                   
                    if (error.message.includes('Failed to fetch')) {
                        this.addMessage('💡 Make sure your server is running with CORS enabled', 'system');
                    }
                }
            }
           
            async loadTables() {
                try {
                    const response = await fetch(`${this.elements.serverUrl.value}/api/fabric/tables`);
                   
                    if (response.ok) {
                        const data = await response.json();
                        this.state.availableTables = data.tables || [];
                        this.renderTables();
                       
                        if (this.state.availableTables.length > 0) {
                            this.addMessage(`📊 Found ${this.state.availableTables.length} tables`, 'system');
                        }
                    }
                } catch (error) {
                    this.addMessage(`❌ Failed to load tables: ${error.message}`, 'system');
                }
            }
           
            renderTables() {
                if (this.state.availableTables.length === 0) {
                    this.elements.tableList.innerHTML = '<div style="text-align: center; color: #6b7280; font-size: 13px; padding: 20px;">No tables found</div>';
                    return;
                }
               
                this.elements.tableList.innerHTML = '';
                this.state.availableTables.forEach(table => {
                    const tableItem = document.createElement('div');
                    tableItem.className = 'table-item';
                    tableItem.textContent = table.full_name;
                    tableItem.onclick = () => this.selectTable(table.full_name, tableItem);
                    this.elements.tableList.appendChild(tableItem);
                });
            }
           
            selectTable(tableName, element) {
                this.elements.tableList.querySelectorAll('.table-item').forEach(item => {
                    item.classList.remove('selected');
                });
               
                element.classList.add('selected');
                this.state.selectedTable = tableName;
                this.elements.messageInput.placeholder = `Ask a question about ${tableName}...`;
                this.addMessage(`📋 Selected table: ${tableName}`, 'system');
            }
           
            async sendMessage() {
                const message = this.elements.messageInput.value.trim();
                if (!message) return;
               
                if (!this.state.selectedTable) {
                    this.addMessage('❌ Please select a table first', 'system');
                    return;
                }
               
                this.addMessage(message, 'user');
                this.elements.messageInput.value = '';
                this.autoResizeTextarea();
               
                this.showTypingIndicator();
               
                try {
                    const rowLimit = this.getCurrentRowLimit();
                   
                    const response = await fetch(`${this.elements.serverUrl.value}/api/fabric/analyze`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        },
                        body: JSON.stringify({
                            question: message,
                            table: this.state.selectedTable,
                            limit: rowLimit
                        })
                    });
                   
                    if (response.ok) {
                        const data = await response.json();
                        this.hideTypingIndicator();
                        this.addMessage(data.analysis || 'No response received', 'assistant');
                    } else {
                        throw new Error(`HTTP ${response.status}`);
                    }
                   
                } catch (error) {
                    this.hideTypingIndicator();
                    this.addMessage(`❌ Query failed: ${error.message}`, 'system');
                }
            }
           
            addMessage(content, type) {
                const messageEl = document.createElement('div');
                messageEl.className = `message ${type}`;
                messageEl.textContent = content;
               
                this.elements.messages.appendChild(messageEl);
                this.scrollToBottom();
            }
           
            showTypingIndicator() {
                this.elements.typingIndicator.style.display = 'block';
                this.elements.messageInput.disabled = true;
                this.elements.sendBtn.disabled = true;
                this.scrollToBottom();
            }
           
            hideTypingIndicator() {
                this.elements.typingIndicator.style.display = 'none';
                this.elements.messageInput.disabled = false;
                this.elements.sendBtn.disabled = false;
                this.elements.messageInput.focus();
            }
           
            updateConnectionStatus(status) {
                const indicator = this.elements.statusIndicator;
                const text = this.elements.statusText;
                const badge = this.elements.connectionBadge;
               
                switch(status) {
                    case 'connecting':
                        indicator.style.background = '#f59e0b';
                        text.textContent = 'Connecting...';
                        badge.textContent = '⚡ CONNECTING';
                        badge.className = 'status-badge connecting';
                        break;
                    case 'connected':
                        indicator.className = 'status-indicator online';
                        text.textContent = 'Server is online';
                        badge.textContent = '✅ CONNECTED';
                        badge.className = 'status-badge connected';
                        break;
                    case 'offline':
                        indicator.className = 'status-indicator';
                        text.textContent = 'Server is offline';
                        badge.textContent = '❌ OFFLINE';
                        badge.className = 'status-badge offline';
                        break;
                }
            }
           
            refreshTables() {
                if (this.state.isConnected) {
                    this.loadTables();
                } else {
                    this.addMessage('❌ Please connect to server first', 'system');
                }
            }
           
            clearChat() {
                this.elements.messages.innerHTML = '<div class="message system">Chat cleared. Ready for new questions!</div>';
            }
           
            scrollToBottom() {
                setTimeout(() => {
                    this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
                }, 100);
            }
        }
       
        // Initialize when page loads
        document.addEventListener('DOMContentLoaded', () => {
            window.chatbot = new DataChatbot();
        });
    </script>
</body>
</html>
    '''
    
    with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(html_content)

def main():
    """Main function to run the Flask application"""
    # Create templates directory and HTML file
    create_templates_dir()
    
    # Configuration
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 5000))
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    print(f"""
🤖 Fabric Lakehouse Analytics Chatbot Server
=============================================
Server starting on: http://{HOST}:{PORT}
Debug mode: {DEBUG}
    
Available endpoints:
- GET  /                       - Main chatbot interface
- GET  /api/fabric/health      - Health check
- GET  /api/fabric/tables      - Get available tables
- POST /api/fabric/analyze     - Analyze data with natural language
- POST /api/fabric/disconnect  - Disconnect from Fabric
    
To customize for real Fabric Lakehouse:
1. Replace FabricLakehouseConnector with actual Fabric connection logic
2. Install Microsoft Fabric SDK/drivers
3. Configure authentication (Azure AD, service principals, etc.)
4. Update the connection string format
5. Implement proper error handling and security

## Environment Variables

Set these environment variables to customize the server:

- `HOST`: Server host (default: 0.0.0.0)
- `PORT`: Server port (default: 5000)  
- `DEBUG`: Debug mode (default: False)

Example:
```bash
export HOST=localhost
export PORT=8080
export DEBUG=true
python fabric_chatbot.py
```

## Features

- Web-based chat interface
- Table selection and browsing
- Configurable row limits
- Real-time connection status
- RESTful API endpoints
- Responsive design
- Error handling and logging

## API Endpoints

- `GET /api/fabric/health` - Check server health
- `GET /api/fabric/tables` - Get available tables
- `POST /api/fabric/analyze` - Analyze data with natural language
- `POST /api/fabric/disconnect` - Disconnect from Fabric

## Security Considerations

For production deployment:
- Add authentication and authorization
- Implement rate limiting
- Use HTTPS
- Validate and sanitize all inputs
- Add proper logging and monitoring
- Configure firewall rules
    """)
    
    try:
        app.run(host=HOST, port=PORT, debug=DEBUG)
    except KeyboardInterrupt:
        print("\n🛑 Server shutdown requested")
    except Exception as e:
        logger.error(f"Server failed to start: {str(e)}")
        raise

if __name__ == '__main__':
    main()


# Additional utility functions and classes for extended functionality

class ConfigManager:
    """Configuration manager for the chatbot application"""
    
    def __init__(self):
        self.config_file = 'chatbot_config.json'
        self.default_config = {
            'server': {
                'host': '0.0.0.0',
                'port': 5000,
                'debug': False
            },
            'fabric': {
                'connection_timeout': 30,
                'query_timeout': 300,
                'max_rows': 1000000
            },
            'logging': {
                'level': 'INFO',
                'file': 'chatbot.log'
            }
        }
    
    def load_config(self):
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            else:
                self.save_config(self.default_config)
                return self.default_config
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            return self.default_config
    
    def save_config(self, config):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save config: {str(e)}")

class QueryProcessor:
    """Advanced query processor for natural language to SQL conversion"""
    
    def __init__(self):
        self.common_queries = {
            'count': 'SELECT COUNT(*) FROM {table} LIMIT {limit}',
            'summary': 'SELECT * FROM {table} LIMIT {limit}',
            'top': 'SELECT * FROM {table} ORDER BY {column} DESC LIMIT {limit}',
            'average': 'SELECT AVG({column}) FROM {table} LIMIT {limit}'
        }
    
    def process_question(self, question: str, table: str, limit: int) -> str:
        """Process natural language question into analysis"""
        question_lower = question.lower()
        
        # Simple keyword matching - in real implementation, use NLP/AI
        if any(word in question_lower for word in ['how many', 'count', 'total']):
            return f"Counting records in {table} (analyzing {limit} rows)..."
        elif any(word in question_lower for word in ['average', 'mean']):
            return f"Calculating averages for {table} (analyzing {limit} rows)..."
        elif any(word in question_lower for word in ['top', 'highest', 'best']):
            return f"Finding top records in {table} (analyzing {limit} rows)..."
        elif any(word in question_lower for word in ['summary', 'overview', 'describe']):
            return f"Providing summary analysis of {table} (analyzing {limit} rows)..."
        else:
            return f"Analyzing {table} for: '{question}' (analyzing {limit} rows)..."

# Installation and setup instructions
SETUP_INSTRUCTIONS = """
# Fabric Lakehouse Analytics Chatbot - Setup Instructions

## Prerequisites
- Python 3.7 or higher
- pip (Python package installer)

## Installation

1. Install required packages:
```bash
pip install flask flask-cors pandas sqlite3
```

2. Save this code as `fabric_chatbot.py`

3. Run the application:
```bash
python fabric_chatbot.py
```

4. Open your browser and go to: http://localhost:5000

## Customization for Real Fabric Lakehouse

To connect to a real Microsoft Fabric Lakehouse:

1. Replace the `FabricLakehouseConnector` class with actual Fabric connection logic
2. Install Microsoft Fabric SDK/drivers
3. Configure authentication (Azure AD, service principals, etc.)
4. Update the connection string format
5. Implement proper error handling and security

## Environment Variables

Set these environment variables to customize the server:

- `HOST`: Server host (default: 0.0.0.0)
- `PORT`: Server port (default: 5000)  
- `DEBUG`: Debug mode (default: False)

Example:
```bash
export HOST=localhost
export PORT=8080
export DEBUG=true
python fabric_chatbot.py
```

## Features

- Web-based chat interface
- Table selection and browsing
- Configurable row limits
- Real-time connection status
- RESTful API endpoints
- Responsive design
- Error handling and logging

## API Endpoints

- `GET /api/fabric/health` - Check server health
- `GET /api/fabric/tables` - Get available tables
- `POST /api/fabric/analyze` - Analyze data with natural language
- `POST /api/fabric/disconnect` - Disconnect from Fabric

## Security Considerations

For production deployment:
- Add authentication and authorization
- Implement rate limiting
- Use HTTPS
- Validate and sanitize all inputs
- Add proper logging and monitoring
- Configure firewall rules
"""