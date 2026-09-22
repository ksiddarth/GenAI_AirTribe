import os
import time
import glob
from pathlib import Path
from typing import Dict, List
from mcp.server.fastmcp import FastMCP

# Initialize JSON-RPC 2.0 compliant MCP Server
mcp = FastMCP(
    name="FileSystemServer",
    description="MCP Server for local filesystem operations and resume monitoring"
)

@mcp.tool()
def read_file(file_path: str) -> str:
    """Read and return the contents of a file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {str(e)}"

@mcp.tool()
def list_directory(directory_path: str) -> List[str]:
    """List all files and folders in a specified directory."""
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    return os.listdir(directory_path)

@mcp.tool()
def watch_directory(directory_path: str, timeout_seconds: int = 15) -> List[str]:
    """
    Monitor for new resumes (files) added to the directory.
    Blocks for timeout_seconds and returns newly detected files.
    """
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    initial_files = set(os.listdir(directory_path))
    time.sleep(timeout_seconds)
    current_files = set(os.listdir(directory_path))
    
    new_files = list(current_files - initial_files)
    return new_files

@mcp.tool()
def batch_process(directory_path: str, pattern: str = "*.txt") -> Dict[str, str]:
    """
    Handle multiple files efficiently. 
    Reads all files matching the pattern and returns a dictionary of filename -> snippet.
    """
    search_pattern = os.path.join(directory_path, pattern)
    matched_files = glob.glob(search_pattern)
    
    results = {}
    for file_path in matched_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Store first 100 characters for efficiency
                results[os.path.basename(file_path)] = f.read(100) + "..." 
        except Exception as e:
            results[os.path.basename(file_path)] = f"Error: {str(e)}"
            
    return results

if __name__ == "__main__":
    # Runs the server using stdio transport (standard for MCP local communication)
    mcp.run()