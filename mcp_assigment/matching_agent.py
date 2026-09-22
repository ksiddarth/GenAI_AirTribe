import asyncio
from typing import Annotated, TypedDict, Sequence
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Define LangGraph State
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], "The chat history"]
    directory_path: str
    processed_resumes: dict

class ResumeMatchingAgent:
    def __init__(self, server_script_path: str):
        # Configure MCP Stdio Client targeting the server script
        self.server_params = StdioServerParameters(
            command="python",
            args=[server_script_path],
            env=None
        )

    async def execute_mcp_tool(self, tool_name: str, arguments: dict) -> any:
        """Connects to MCP server, calls a tool, and returns the result."""
        async with stdio_client(self.server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments=arguments)
                return result.content[0].text

    async def scan_and_batch_node(self, state: AgentState) -> AgentState:
        """Calls MCP batch_process to ingest multiple resumes."""
        print("[Agent] Requesting batch processing from MCP Server...")
        result = await self.execute_mcp_tool(
            "batch_process", 
            {"directory_path": state["directory_path"], "pattern": "*.txt"}
        )
        return {"processed_resumes": result}

    async def watch_node(self, state: AgentState) -> AgentState:
        """Calls MCP watch_directory to wait for new inbound resumes."""
        print("[Agent] Watching directory for new resumes via MCP...")
        new_files = await self.execute_mcp_tool(
            "watch_directory",
            {"directory_path": state["directory_path"], "timeout_seconds": 5}
        )
        return {"messages": [AIMessage(content=f"New files detected: {new_files}")]}

    def build_graph(self):
        """Construct the LangGraph workflow."""
        workflow = StateGraph(AgentState)
        
        workflow.add_node("batch_process_resumes", self.scan_and_batch_node)
        workflow.add_node("watch_for_new", self.watch_node)
        
        workflow.set_entry_point("batch_process_resumes")
        workflow.add_edge("batch_process_resumes", "watch_for_new")
        workflow.add_edge("watch_for_new", END)
        
        return workflow.compile()

# Execution entry point
if __name__ == "__main__":
    agent = ResumeMatchingAgent("filesystem_mcp_server.py")
    graph = agent.build_graph()
    
    initial_state = AgentState(
        messages=[HumanMessage(content="Start resume processing")],
        directory_path="./resumes",
        processed_resumes={}
    )
    
    # Run async LangGraph
    result = asyncio.run(graph.ainvoke(initial_state))
    print("\nFinal State:")
    print(result["processed_resumes"])
    print(result["messages"][-1].content)