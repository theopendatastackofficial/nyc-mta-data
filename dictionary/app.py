import os
import sys
import socket
import sqlite3
from flask import Flask, render_template, jsonify, request

# Add the parent directory to the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.constants import SQLITE_PATH

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
sqlite_file_path = SQLITE_PATH

def get_table_metadata(table_name):
    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()

    query = """
    SELECT column_name, data_type, description
    FROM duckdb_schema
    WHERE table_name = ?;
    """
    
    cursor.execute(query, (table_name,))
    rows = cursor.fetchall()
    conn.close()
    
    metadata_human = []
    metadata_llm = f"Table: {table_name}\n"
    
    for row in rows:
        column_name, data_type, description = row
        # Human-readable version
        metadata_human.append({
            "column_name": column_name,
            "data_type": data_type,
            "description": description or "No description available"
        })
        
        # LLM-optimized version
        metadata_llm += f"{column_name}: {data_type}, {description or 'No description'}; "
    
    return metadata_human, metadata_llm

@app.route('/')
def index():
    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT table_name FROM duckdb_schema;")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return render_template('index.html', tables=tables)

@app.route('/table/<table_name>')
def table_metadata(table_name):
    metadata_human, metadata_llm = get_table_metadata(table_name)
    return jsonify({
        "human": metadata_human,
        "llm": metadata_llm
    })

def find_open_port(start_port=5000, max_port=65535):
    """
    Tries to find an open port, starting at `start_port` up to `max_port`.
    Returns the first open port found or raises an error if none are available.
    """
    for port in range(start_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # If connect_ex returns 0, the port is in use; otherwise it's free
            if sock.connect_ex(('127.0.0.1', port)) != 0:
                return port
    raise RuntimeError("No open ports available in range.")

if __name__ == '__main__':
    # Continuously check for an available port from 5000 upward
    open_port = find_open_port(5000, 65535)
    print(f"Launching Flask on port {open_port}")
    app.run(debug=False, port=open_port)
