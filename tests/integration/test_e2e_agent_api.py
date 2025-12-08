"""End-to-end integration tests for agent API."""

import json
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from dotenv import load_dotenv
from fastapi.testclient import TestClient

from src.app.agent_api.main import app
from src.app.agent_api.tool_catalog import fetch_tool_catalog
from src.etl.database.connection import DatabaseConnection

# Load environment variables from .env file
load_dotenv()

# Configure logging for debugging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

client = TestClient(app)

# MCP server configuration - use a different port for testing to avoid conflicts
MCP_SERVER_PORT = int(os.getenv("TEST_MCP_SERVER_PORT", "8001"))
MCP_SERVER_NAME = os.getenv("TEST_MCP_SERVER_NAME")
MCP_SERVER_URL = f"http://localhost:{MCP_SERVER_PORT}/mcp"
MCP_SERVER_TIMEOUT = 10  # seconds to wait for server to start

logger.info(f"Test MCP Server Configuration: PORT={MCP_SERVER_PORT}, NAME={MCP_SERVER_NAME}, URL={MCP_SERVER_URL}")

# Module-level variable to store MCP server process
_mcp_server_process = None


@pytest.fixture(scope="session", autouse=True)
def check_llm_config():
    """Check if LLM is configured, raise error if not."""
    model = os.getenv("LARGE_LANGUAGE_MODEL")
    provider = os.getenv("LLM_PROVIDER")
    if not model or not provider:
        raise RuntimeError(
            "LARGE_LANGUAGE_MODEL and LLM_PROVIDER environment variables must be set for integration tests.\n"
            "Example: export LARGE_LANGUAGE_MODEL=gpt-4; export LLM_PROVIDER=openai"
        )


@pytest.fixture(scope="session", autouse=True)
def check_test_database():
    """Check if test database is configured, raise error if not."""
    test_db_name = os.getenv("TEST_DB_NAME")
    if not test_db_name:
        raise RuntimeError(
            "TEST_DB_NAME environment variable must be set for integration tests.\n"
            "Example: export TEST_DB_NAME=mtg_meta_mage_test"
        )


@pytest.fixture(scope="session", autouse=True)
def start_mcp_server():
    """
    Start MCP server as a subprocess at the beginning of test session.
    
    Raises RuntimeError if server cannot be started or becomes unavailable.
    """
    global _mcp_server_process
    import shutil
    
    # Check if fastmcp is available
    fastmcp_path = shutil.which("fastmcp")
    if not fastmcp_path:
        # Try with uv run
        uv_path = shutil.which("uv")
        if not uv_path:
            raise RuntimeError(
                "Cannot start MCP server: 'fastmcp' command not found.\n"
                "Install it with: pip install fastmcp\n"
                "Or ensure 'uv' is available to run: uv run fastmcp"
            )
        cmd = ["uv", "run", "fastmcp", "run", "src/app/mcp/server.py", "--transport", "http", "--port", str(MCP_SERVER_PORT)]
    else:
        cmd = [fastmcp_path, "run", "src/app/mcp/server.py", "--transport", "http", "--port", str(MCP_SERVER_PORT)]
    
    # Set MCP server environment variables for the test
    os.environ["MCP_SERVER_NAME"] = MCP_SERVER_NAME
    os.environ["MCP_SERVER_PORT"] = str(MCP_SERVER_PORT)
    os.environ["MCP_SERVER_URL"] = MCP_SERVER_URL
    # Also set TEST_ prefixed vars so tool_catalog can use them
    os.environ["TEST_MCP_SERVER_NAME"] = MCP_SERVER_NAME
    os.environ["TEST_MCP_SERVER_PORT"] = str(MCP_SERVER_PORT)
    
    logger.info(f"Set environment variables: MCP_SERVER_NAME={MCP_SERVER_NAME}, MCP_SERVER_PORT={MCP_SERVER_PORT}, MCP_SERVER_URL={MCP_SERVER_URL}")
    
    # Check if port is already in use
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    port_in_use = sock.connect_ex(('localhost', MCP_SERVER_PORT)) == 0
    sock.close()
    
    if port_in_use:
            # Port is in use - check if it's our MCP server by trying to fetch catalog
            logger.warning(f"Port {MCP_SERVER_PORT} is already in use. Checking if it's a working MCP server...")
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    catalog = loop.run_until_complete(asyncio.wait_for(fetch_tool_catalog(), timeout=2.0))
                    if catalog and len(catalog) > 0:
                        logger.info(f"Found existing MCP server on port {MCP_SERVER_PORT} with {len(catalog)} tools, reusing it")
                        loop.close()
                        return  # Server is already running and working
                    else:
                        logger.warning(f"Existing server on port {MCP_SERVER_PORT} responded but has 0 tools. Will start new server.")
                except Exception as e:
                    logger.debug(f"Existing server on port {MCP_SERVER_PORT} is not responding correctly: {e}")
                finally:
                    loop.close()
            except Exception as e:
                logger.debug(f"Error checking existing server: {e}")
            
            # If we get here, the existing server is not suitable - kill it and start fresh
            logger.info(f"Killing existing process on port {MCP_SERVER_PORT} to start fresh server...")
            import socket
            import subprocess
            try:
                # Find and kill the process using the port
                result = subprocess.run(
                    ["lsof", "-ti", f":{MCP_SERVER_PORT}"],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0 and result.stdout.strip():
                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        try:
                            os.kill(int(pid), 15)  # SIGTERM
                            logger.info(f"Sent SIGTERM to process {pid}")
                        except (ProcessLookupError, ValueError) as e:
                            logger.debug(f"Could not kill process {pid}: {e}")
                    # Wait a moment for processes to terminate
                    time.sleep(2)
                    # Check if port is now free
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    still_in_use = sock.connect_ex(('localhost', MCP_SERVER_PORT)) == 0
                    sock.close()
                    if still_in_use:
                        logger.warning(f"Port {MCP_SERVER_PORT} still in use after kill attempt, will try to start server anyway")
                else:
                    logger.debug(f"No process found using port {MCP_SERVER_PORT}")
            except Exception as e:
                logger.warning(f"Error killing existing process: {e}, will try to start server anyway")
    
    # Start the server
    logger.info(f"Starting MCP server on port {MCP_SERVER_PORT}...")
    logger.debug(f"Command: {' '.join(cmd)}")
    logger.debug(f"Working directory: {Path(__file__).parent.parent.parent}")
    logger.debug(f"Environment MCP_SERVER_NAME: {os.environ.get('MCP_SERVER_NAME')}")
    # Use unbuffered output so we can see logs in real-time
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combine stderr into stdout
        cwd=Path(__file__).parent.parent.parent,
        env=os.environ.copy(),
        text=True,  # Text mode for easier decoding
        bufsize=0,  # Unbuffered
    )
    
    _mcp_server_process = process
    
    # Start a thread to read server output in real-time
    import threading
    server_output_lines = []
    output_lock = threading.Lock()
    
    def read_server_output():
        """Read server output and log it."""
        try:
            for line in iter(process.stdout.readline, ''):
                if not line:
                    break
                line = line.rstrip()
                with output_lock:
                    server_output_lines.append(line)
                logger.debug(f"[MCP Server] {line}")
        except Exception as e:
            logger.debug(f"Error reading server output: {e}")
    
    output_thread = threading.Thread(target=read_server_output, daemon=True)
    output_thread.start()
    
    # Wait for server to be ready
    logger.info("Waiting for MCP server to be ready...")
    start_time = time.time()
    server_ready = False
    
    # Create a single event loop for all async checks
    import asyncio
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def check_server_ready():
            """Check if server is ready by fetching tool catalog."""
            try:
                logger.debug(f"Checking if MCP server is ready at {MCP_SERVER_URL}...")
                catalog = await asyncio.wait_for(fetch_tool_catalog(), timeout=3.0)
                is_ready = catalog is not None and len(catalog) > 0
                if is_ready:
                    logger.info(f"MCP server is ready! Found {len(catalog)} tools")
                else:
                    logger.warning(f"MCP server responded but catalog is empty or None")
                return is_ready
            except (asyncio.TimeoutError, Exception) as e:
                logger.debug(f"MCP server not ready yet: {type(e).__name__}: {e}")
                return False
        
        while time.time() - start_time < MCP_SERVER_TIMEOUT:
            # Check if process is still running
            if process.poll() is not None:
                # Process exited, wait a moment for output thread to finish
                time.sleep(0.5)
                with output_lock:
                    error_msg = "\n".join(server_output_lines) if server_output_lines else "(no output captured)"
                _mcp_server_process = None
                if loop:
                    loop.close()
                raise RuntimeError(
                    f"MCP server failed to start. Exit code: {process.returncode}\n"
                    f"Command: {' '.join(cmd)}\n"
                    f"Error output:\n{error_msg}"
                )
            
            # First check if HTTP endpoint is responding (quick check)
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                result = sock.connect_ex(('localhost', MCP_SERVER_PORT))
                sock.close()
                if result == 0:
                    logger.debug(f"Port {MCP_SERVER_PORT} is open, checking MCP protocol...")
                    # Port is open, now try MCP protocol check with timeout
                    try:
                        if loop.run_until_complete(check_server_ready()):
                            server_ready = True
                            logger.info("MCP server is ready and responding!")
                            break
                    except Exception as e:
                        logger.debug(f"Error checking MCP server protocol: {type(e).__name__}: {e}")
                else:
                    logger.debug(f"Port {MCP_SERVER_PORT} is not open yet")
            except Exception as e:
                logger.debug(f"Error checking server port: {type(e).__name__}: {e}")
            
            time.sleep(0.5)
    finally:
        if loop:
            loop.close()
    
    if not server_ready:
        # Give output thread a moment to finish reading
        time.sleep(0.5)
        
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        _mcp_server_process = None
        
        with output_lock:
            output_summary = "\n".join(server_output_lines[-30:]) if server_output_lines else "(no output captured)"
        raise RuntimeError(
            f"MCP server did not become ready within {MCP_SERVER_TIMEOUT} seconds.\n"
            f"Server URL: {MCP_SERVER_URL}\n"
            f"Server Name: {MCP_SERVER_NAME}\n"
            f"Server Port: {MCP_SERVER_PORT}\n"
            f"Last server output:\n{output_summary}\n"
            f"Check server logs for errors."
        )
    
    logger.info("MCP server is ready")


@pytest.fixture(scope="session", autouse=True)
def stop_mcp_server():
    """
    Stop MCP server at the end of test session.
    This fixture yields immediately so it's set up early, then cleans up at the end.
    """
    global _mcp_server_process
    
    yield
    
    if _mcp_server_process is None:
        logger.warning("MCP server process not found, skipping cleanup")
        return
    
    logger.info("Stopping MCP server...")
    try:
        _mcp_server_process.terminate()
        try:
            _mcp_server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("MCP server did not terminate gracefully, killing...")
            _mcp_server_process.kill()
            _mcp_server_process.wait()
    except Exception as e:
        logger.warning(f"Error stopping MCP server: {e}")
        try:
            _mcp_server_process.kill()
        except Exception:
            pass
    
    _mcp_server_process = None
    logger.info("MCP server stopped")


@pytest.fixture(scope="session", autouse=True)
def verify_mcp_server(start_mcp_server):
    """
    Verify MCP server is working correctly after startup.
    Raises RuntimeError if server is not functioning properly.
    """
    import asyncio
    
    async def verify():
        try:
            logger.info(f"Verifying MCP server at {MCP_SERVER_URL}...")
            catalog = await fetch_tool_catalog()
            if not catalog:
                raise RuntimeError("MCP server returned empty tool catalog")
            if len(catalog) == 0:
                raise RuntimeError("MCP server returned no tools")
            logger.info(f"MCP server verified: {len(catalog)} tools available")
            logger.debug(f"Available tools: {[t.get('name', 'unknown') for t in catalog]}")
            return True
        except Exception as e:
            logger.error(f"MCP server verification failed: {type(e).__name__}: {e}", exc_info=True)
            raise RuntimeError(
                f"MCP server is not functioning correctly: {e}\n"
                f"Server URL: {MCP_SERVER_URL}\n"
                f"Server Name: {MCP_SERVER_NAME}\n"
                f"Server Port: {MCP_SERVER_PORT}\n"
                f"Check that the server started successfully and is responding."
            )
    
    asyncio.run(verify())


@pytest.fixture(autouse=True)
def reset_llm_client_cache():
    """Reset LLM client cache between tests to ensure fresh LLM calls."""
    # Clear the cached intent client so each test gets a fresh LLM call
    import src.app.agent_api.graph as graph_module
    graph_module._intent_client = None
    yield
    # Clean up after test
    graph_module._intent_client = None


@pytest.fixture(autouse=True)
def reset_tool_catalog_cache():
    """Reset tool catalog cache between tests."""
    import src.app.agent_api.tool_catalog as tool_catalog_module
    tool_catalog_module._catalog_cache = None
    yield
    tool_catalog_module._catalog_cache = None


@pytest.fixture
def sample_meta_data(test_database):
    """
    Create sample tournament, archetype, and match data for agent API tests.
    
    Creates:
    - 2 tournaments (1 recent, 1 older) in Modern format
    - 3 archetype groups (murktide, rhinos, hammer)
    - 10 decklists across both tournaments
    - 15 matches with winners
    """
    with DatabaseConnection.transaction() as conn:
        cur = conn.cursor()
        
        # Use Modern format
        now = datetime.now(timezone.utc)
        recent_date = now - timedelta(days=7)  # Within current period
        older_date = now - timedelta(days=30)  # Older period
        
        # Create tournaments in Modern format
        cur.execute("""
            INSERT INTO tournaments (tournament_id, tournament_name, format, start_date, swiss_num, top_cut)
            VALUES 
                ('t1', 'Modern Showdown 1', 'Modern', %s, 5, 8),
                ('t2', 'Modern Showdown 2', 'Modern', %s, 5, 8)
        """, (recent_date, older_date))
        
        # Create Modern archetype groups
        cur.execute("""
            INSERT INTO archetype_groups (archetype_group_id, format, main_title, color_identity, strategy)
            VALUES 
                (1, 'Modern', 'murktide', 'UR', 'tempo'),
                (2, 'Modern', 'rhinos', 'WUBRG', 'cascade'),
                (3, 'Modern', 'hammer', 'W', 'aggro')
        """)
        
        # Create players for tournament 1 (recent)
        players_t1 = []
        for i in range(1, 6):
            player_id = f't1_p{i}'
            players_t1.append(player_id)
            cur.execute("""
                INSERT INTO players (player_id, tournament_id, name, wins, losses, standing)
                VALUES (%s, 't1', %s, 3, 2, %s)
            """, (player_id, f'Player {i}', i))
        
        # Create players for tournament 2 (older)
        players_t2 = []
        for i in range(1, 6):
            player_id = f't2_p{i}'
            players_t2.append(player_id)
            cur.execute("""
                INSERT INTO players (player_id, tournament_id, name, wins, losses, standing)
                VALUES (%s, 't2', %s, 3, 2, %s)
            """, (player_id, f'Player {i}', i))
        
        # Create decklists for tournament 1
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t1_p1', 't1', 1),
                ('t1_p2', 't1', 1),
                ('t1_p3', 't1', 2),
                ('t1_p4', 't1', 2),
                ('t1_p5', 't1', 3)
        """)
        
        # Create decklists for tournament 2
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t2_p1', 't2', 1),
                ('t2_p2', 't2', 2),
                ('t2_p3', 't2', 2),
                ('t2_p4', 't2', 3),
                ('t2_p5', 't2', 3)
        """)
        
        # Create match rounds
        cur.execute("""
            INSERT INTO match_rounds (round_number, tournament_id, round_description)
            VALUES 
                (1, 't1', 'Round 1'),
                (2, 't1', 'Round 2'),
                (3, 't1', 'Round 3'),
                (1, 't2', 'Round 1'),
                (2, 't2', 'Round 2'),
                (3, 't2', 'Round 3')
        """)
        
        # Create matches for tournament 1
        cur.execute("""
            INSERT INTO matches (round_number, tournament_id, match_num, player1_id, player2_id, winner_id, status)
            VALUES 
                (1, 't1', 1, 't1_p1', 't1_p3', 't1_p1', 'completed'),
                (1, 't1', 2, 't1_p2', 't1_p4', 't1_p2', 'completed'),
                (2, 't1', 1, 't1_p1', 't1_p2', 't1_p1', 'completed'),
                (2, 't1', 2, 't1_p3', 't1_p5', 't1_p5', 'completed'),
                (3, 't1', 1, 't1_p5', 't1_p4', 't1_p5', 'completed')
        """)
        
        # Create matches for tournament 2
        cur.execute("""
            INSERT INTO matches (round_number, tournament_id, match_num, player1_id, player2_id, winner_id, status)
            VALUES 
                (1, 't2', 1, 't2_p1', 't2_p2', 't2_p1', 'completed'),
                (1, 't2', 2, 't2_p3', 't2_p4', 't2_p4', 'completed'),
                (2, 't2', 1, 't2_p1', 't2_p3', 't2_p1', 'completed'),
                (2, 't2', 2, 't2_p2', 't2_p5', 't2_p5', 'completed'),
                (3, 't2', 1, 't2_p3', 't2_p4', 't2_p3', 'completed')
        """)
        
        cur.close()


@pytest.mark.integration
class TestFullConversationFlow:
    """Integration tests for full conversation flow."""

    def test_new_conversation_flow(self, sample_meta_data):
        """Test starting a new conversation and receiving SSE response."""
        payload = {
            "message": "What's the Modern meta?",
            "conversation_id": None,
            "context": {"format": "Modern", "days": 30},
        }
        
        response = client.post("/chat", json=payload)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        
        # Parse SSE events
        body = response.text
        events = {}
        for line in body.split("\n"):
            if line.startswith("event:"):
                event_type = line.replace("event:", "").strip()
            elif line.startswith("data:"):
                data_str = line.replace("data:", "").strip()
                if data_str:
                    events[event_type] = json.loads(data_str)
        
        # Verify required events
        assert "metadata" in events
        assert "done" in events
        assert "content" in events
        
        # Verify metadata structure
        metadata = events["metadata"]
        assert "conversation_id" in metadata
        assert metadata["format"] == "Modern"
        
        # Verify conversation can be retrieved
        conversation_id = metadata["conversation_id"]
        get_response = client.get(f"/conversations/{conversation_id}")
        assert get_response.status_code == 200
        convo_data = get_response.json()
        assert convo_data["conversation_id"] == conversation_id
        assert convo_data["state"]["format"] == "Modern"
        assert len(convo_data["messages"]) >= 2  # User message + assistant response

    def test_multi_turn_conversation(self, sample_meta_data):
        """Test continuing a conversation across multiple turns."""
        # First turn
        payload1 = {
            "message": "What's the Modern meta?",
            "conversation_id": None,
            "context": {"format": "Modern", "days": 30},
        }
        response1 = client.post("/chat", json=payload1)
        assert response1.status_code == 200
        
        # Extract conversation ID from SSE
        body1 = response1.text
        conversation_id = None
        for line in body1.split("\n"):
            if line.startswith("data:") and "conversation_id" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    conversation_id = data.get("conversation_id")
                    break
        
        assert conversation_id is not None
        
        # Second turn
        payload2 = {
            "message": "Tell me more about the top deck",
            "conversation_id": conversation_id,
            "context": {},
        }
        response2 = client.post("/chat", json=payload2)
        assert response2.status_code == 200
        
        # Verify conversation state persisted
        get_response = client.get(f"/conversations/{conversation_id}")
        assert get_response.status_code == 200
        convo_data = get_response.json()
        assert len(convo_data["messages"]) >= 4  # 2 user + 2 assistant messages

    def test_conversation_state_persistence(self, sample_meta_data):
        """Test that conversation state persists across requests."""
        payload = {
            "message": "I want to analyze Modern",
            "conversation_id": None,
            "context": {"format": "Modern", "days": 30},
        }
        
        response = client.post("/chat", json=payload)
        assert response.status_code == 200
        
        # Extract conversation ID
        body = response.text
        conversation_id = None
        for line in body.split("\n"):
            if line.startswith("data:") and "conversation_id" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    conversation_id = data.get("conversation_id")
                    break
        
        assert conversation_id is not None
        
        # Retrieve conversation
        get_response = client.get(f"/conversations/{conversation_id}")
        assert get_response.status_code == 200
        convo_data = get_response.json()
        
        # Verify state
        assert convo_data["state"]["format"] == "Modern"
        assert convo_data["state"]["days"] == 30
        assert convo_data["state"]["has_deck"] is False


@pytest.mark.integration
class TestWorkflowInterleaving:
    """Integration tests for workflow interleaving scenarios."""

    def test_meta_to_deck_coaching_transition(self, sample_meta_data):
        """Test transitioning from meta research to deck coaching."""
        # Start with meta research
        payload1 = {
            "message": "What's the Modern meta?",
            "conversation_id": None,
            "context": {"format": "Modern", "days": 30},
        }
        response1 = client.post("/chat", json=payload1)
        assert response1.status_code == 200
        
        # Extract conversation ID
        body1 = response1.text
        conversation_id = None
        for line in body1.split("\n"):
            if line.startswith("data:") and "conversation_id" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    conversation_id = data.get("conversation_id")
                    break
        
        assert conversation_id is not None
        
        # Switch to deck coaching
        payload2 = {
            "message": "Now help me with my deck",
            "conversation_id": conversation_id,
            "context": {"deck_text": "4 Lightning Bolt\n4 Lava Spike"},
        }
        response2 = client.post("/chat", json=payload2)
        assert response2.status_code == 200
        
        # Verify state updated
        get_response = client.get(f"/conversations/{conversation_id}")
        assert get_response.status_code == 200
        convo_data = get_response.json()
        # Format should persist from first turn
        assert convo_data["state"]["format"] == "Modern"

    def test_deck_to_meta_transition(self, sample_meta_data):
        """Test transitioning from deck coaching to meta research."""
        # Start with deck coaching intent
        payload1 = {
            "message": "Help me optimize my Modern deck",
            "conversation_id": None,
            "context": {"format": "Modern", "deck_text": "4 Lightning Bolt"},
        }
        response1 = client.post("/chat", json=payload1)
        assert response1.status_code == 200
        
        # Extract conversation ID
        body1 = response1.text
        conversation_id = None
        for line in body1.split("\n"):
            if line.startswith("data:") and "conversation_id" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    conversation_id = data.get("conversation_id")
                    break
        
        assert conversation_id is not None
        
        # Switch to meta research
        payload2 = {
            "message": "Actually, tell me about the format meta instead",
            "conversation_id": conversation_id,
            "context": {"days": 30},
        }
        response2 = client.post("/chat", json=payload2)
        assert response2.status_code == 200
        
        # Verify state persisted
        get_response = client.get(f"/conversations/{conversation_id}")
        assert get_response.status_code == 200
        convo_data = get_response.json()
        assert convo_data["state"]["format"] == "Modern"


@pytest.mark.integration
class TestBlockingDependencyEnforcement:
    """Integration tests for blocking dependency enforcement."""

    def test_missing_format_blocked(self):
        """Test that missing format is blocked."""
        payload = {
            "message": "What's the meta?",
            "conversation_id": None,
            "context": {},  # No format provided
        }
        
        response = client.post("/chat", json=payload)
        assert response.status_code == 200
        
        # Parse SSE to get assistant message
        body = response.text
        content_found = False
        for line in body.split("\n"):
            if line.startswith("data:") and "text" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    if "Format is required" in data.get("text", ""):
                        content_found = True
                        break
        
        assert content_found, "Should block when format is missing"

    def test_missing_days_for_meta_research_blocked(self):
        """Test that missing days is blocked for meta research."""
        payload = {
            "message": "What's the Modern meta?",
            "conversation_id": None,
            "context": {"format": "Modern"},  # No days provided
        }
        
        response = client.post("/chat", json=payload)
        assert response.status_code == 200
        
        # Parse SSE to get assistant message
        body = response.text
        content_found = False
        for line in body.split("\n"):
            if line.startswith("data:") and "text" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    if "days" in data.get("text", "").lower():
                        content_found = True
                        break
        
        assert content_found, "Should block when days is missing for meta research"

    def test_missing_deck_for_deck_coaching_blocked(self):
        """Test that missing deck is blocked for deck coaching."""
        payload = {
            "message": "Help me optimize my deck",
            "conversation_id": None,
            "context": {"format": "Modern"},  # No deck_text provided
        }
        
        response = client.post("/chat", json=payload)
        assert response.status_code == 200
        
        # Parse SSE to get assistant message
        body = response.text
        content_found = False
        for line in body.split("\n"):
            if line.startswith("data:") and "text" in line:
                data_str = line.replace("data:", "").strip()
                if data_str:
                    data = json.loads(data_str)
                    if "deck" in data.get("text", "").lower():
                        content_found = True
                        break
        
        assert content_found, "Should block when deck is missing for deck coaching"

