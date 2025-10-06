import ftrack_api
import logging
import os
import signal
import sys
from multiprocessing import Process
from dotenv import load_dotenv

# Import the register functions from your action files
from actions.shot_creation_action import register as register_shot_automation
from actions.template_action import register as register_project_copy
from actions.undark_pbv_sync import register as register_undark_pbv_sync

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- Functions to run each listener ---
def run_listener(register_function, name):
    """Initializes a session and runs a listener function."""
    logger.info(f"Starting listener process: {name}")
    try:
        # Each process must load the .env file to get credentials
        load_dotenv()
        # Each process gets its own session
        session = ftrack_api.Session(auto_connect_event_hub=True)
        register_function(session)
        logger.info(f"Listener '{name}' is waiting for events.")
        session.event_hub.wait()
    except Exception as e:
        logger.error(f"Listener '{name}' failed: {e}", exc_info=True)
        sys.exit(1)

# --- Main execution block ---
if __name__ == '__main__':
    logger.info("Launching ftrack action server...")

    # A list of all actions to run
    actions_to_run = [
        (register_shot_automation, "Shot Creation Automation"),
        (register_project_copy, "Project Copy Action"),
        (register_undark_pbv_sync, "Undark PBV Sync Listener")
    ]

    processes = []
    for register_func, name in actions_to_run:
        process = Process(target=run_listener, args=(register_func, name))
        processes.append(process)
        process.start()

    logger.info(f"{len(processes)} action processes have started.")

    # Graceful shutdown handler
    def shutdown(signum, frame):
        logger.info("Shutdown signal received. Terminating processes...")
        for p in processes:
            p.terminate()
            p.join()
        logger.info("Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for p in processes:
        p.join()