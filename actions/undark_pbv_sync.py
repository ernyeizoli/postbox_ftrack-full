import os
import threading
import logging
from dotenv import load_dotenv
import ftrack_api
import functools

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# --- Load environment variables ---
load_dotenv()

# --- FTRACK API credentials for both instances ---
PBV_FTRACK_API_KEY = os.getenv('FTRACK_API_KEY')
PBV_FTRACK_API_USER = os.getenv('FTRACK_API_USER')
PBV_FTRACK_API_URL = os.getenv('FTRACK_SERVER')

UNDARK_FTRACK_API_KEY = os.getenv('UNDARK_FTRACK_API_KEY')
UNDARK_FTRACK_API_USER = os.getenv('UNDARK_FTRACK_API_USER')
UNDARK_FTRACK_API_URL = os.getenv('UNDARK_FTRACK_API_URL')

def get_ftrack_session(api_key, api_user, api_url):
    """Initializes and returns an ftrack API session."""
    return ftrack_api.Session(
        api_key=api_key,
        api_user=api_user,
        server_url=api_url,
        auto_connect_event_hub=True
    )

def sync_event_handler(session_pbv, session_undark, event):
    """Callback function to handle ftrack events for synchronization."""
    logger.info("Event received.")
    for entity in event['data'].get('entities', []):
        action = entity.get('action')
        entity_type = entity.get('entity_type', '').lower()

        # Route event to the correct handler
        if entity_type == 'task' and action == 'add':
            handle_task_creation(entity, session_pbv, session_undark)
        elif entity_type == 'note' and action in ['add', 'update']:
            handle_note_creation(entity, session_pbv, session_undark)
        elif entity_type == 'assetversion' and action == 'add':
            handle_version_creation(entity, session_pbv, session_undark)
def handle_version_creation(entity, session_pbv, session_undark):
    """
    Syncs AssetVersion creation between servers with a single attempt.
    This version uses specific projections to fetch all required data at once.
    """
    version_id = entity.get('entityId')
    if not version_id:
        logger.warning("[VERSION SYNC] Event is missing version_id. Skipping.")
        return

    logger.info(f"[VERSION SYNC] Processing event for version_id: {version_id}")

    # This specific query is still essential to get the asset and project info.
    query_projection = (
        'select name, comment, status, user, '
        'asset.name, asset.project.name, asset.project.id '
        'from AssetVersion '
        f'where id is "{version_id}"'
    )

    source_session = None
    target_session = None
    source_name = None

    # Determine the source server of the event
    if session_pbv.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_pbv
        target_session = session_undark
        source_name = "PBV"
    elif session_undark.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_undark
        target_session = session_pbv
        source_name = "UNDARK"
    else:
        logger.error(f"[VERSION SYNC] Version ID {version_id} not found on either server.")
        return

    # Perform the query a single time
    source_version = source_session.query(query_projection).first()

    # Check if the data is complete. If not, exit.
    if not source_version or not source_version.get('name'):
        logger.error(f"[VERSION SYNC] Failed to get complete version data from {source_name} for {version_id}. The data may not have been ready. Skipping.")
        return

    # Extract data (this is now safe because of the projection)
    project_name = source_version['asset']['project']['name']
    asset_name = source_version['asset']['name']
    version_name = source_version['name']
    target_name = "UNDARK" if source_name == "PBV" else "PBV"

    logger.info(f"[VERSION SYNC] Source {source_name}: '{project_name}' > '{asset_name}' > '{version_name}'")

    try:
        # Find the corresponding project and asset on the target server
        target_project = target_session.query(f'Project where name is "{project_name}"').first()
        if not target_project:
            logger.warning(f"[VERSION SYNC] Project '{project_name}' not found on target {target_name}. Skipping.")
            return

        target_asset = target_session.query(f'Asset where name is "{asset_name}" and project.id is "{target_project["id"]}"').first()
        if not target_asset:
            logger.warning(f"[VERSION SYNC] Asset '{asset_name}' not found on target {target_name}. Skipping.")
            return

        # Check if version already exists on the target
        existing_version = target_session.query(f'AssetVersion where name is "{version_name}" and asset.id is "{target_asset["id"]}"').first()
        if existing_version:
            logger.info(f"[VERSION SYNC] Version '{version_name}' already exists on target {target_name}. Skipping.")
            return

        # Create the new version on the target server
        target_session.create('AssetVersion', {
            'name': version_name,
            'asset': target_asset,
            'status': source_version['status'],
            'comment': source_version['comment'],
            'user': source_version['user']
        })
        target_session.commit()
        logger.info(f"[VERSION SYNC] SUCCESS: Synced version '{version_name}' to {target_name}.")

    except Exception as e:
        logger.error(f"[VERSION SYNC] An unexpected error occurred during sync to {target_name}: {e}", exc_info=True)

def handle_task_creation(entity, session_pbv, session_undark):
    """Handles one-way sync of a new task from PBV to UNDARK."""
    task_id = entity.get('entityId')
    if not task_id:
        return

    try:
        task = session_pbv.query(f'Task where id is "{task_id}"').one()
        if 'asset-request' not in task['name'].lower():
            return

        project_name = task['project']['name']
        logger.info(f"Processing 'asset-request' task creation: '{task['name']}' in project '{project_name}'")

        target_project = session_undark.query(f'Project where name is "{project_name}"').first()
        if not target_project:
            logger.warning(f"Project '{project_name}' not found in UNDARK. Cannot sync task.")
            return

        # Check if task already exists in the target project
        existing_task = session_undark.query(
            f'Task where name is "{task["name"]}" and parent.id is "{target_project["id"]}"'
        ).first()

        if not existing_task:
            session_undark.create('Task', {'name': task['name'], 'parent': target_project})
            session_undark.commit()
            logger.info(f"SUCCESS: Synced task '{task['name']}' to UNDARK.")
        else:
            logger.info(f"Task '{task['name']}' already exists in UNDARK. Skipping.")

    except Exception as e:
        logger.error(f"Error processing task creation sync: {e}")

def handle_note_creation(entity, session_pbv, session_undark):
    """Handle Note add/update events safely.

    This implementation is intentionally conservative: it logs the incoming
    note and its parent, and does not perform an automatic create on the
    target server. Automatic syncing of notes can produce duplicates and
    may need mapping logic for different parent types; implement that when
    requirements are clear.
    """
    note_id = entity.get('entityId')
    if not note_id:
        logger.warning("[NOTE SYNC] Event missing note id. Skipping.")
        return

    try:
        # Determine source server where the note exists
        source_session = None
        source_name = None
        if session_pbv.query(f'Note where id is "{note_id}"').first():
            source_session = session_pbv
            source_name = 'PBV'
            target_session = session_undark
            target_name = 'UNDARK'
        elif session_undark.query(f'Note where id is "{note_id}"').first():
            source_session = session_undark
            source_name = 'UNDARK'
            target_session = session_pbv
            target_name = 'PBV'
        else:
            logger.warning(f"[NOTE SYNC] Note ID {note_id} not found on either server. Skipping.")
            return

        note = source_session.query(f'Note where id is "{note_id}"').first()
        if not note:
            logger.warning(f"[NOTE SYNC] Failed to fetch Note {note_id} from {source_name}.")
            return

        # Log a short summary to aid debugging. Avoid syncing automatically.
        parent = note.get('parent') or note.get('entity')
        parent_info = f"type={getattr(parent, 'entity_type', None)} id={getattr(parent, 'id', None)}" if parent else 'unknown'
        logger.info(f"[NOTE SYNC] Note {note_id} from {source_name}. Subject: {note.get('subject')} Parent: {parent_info}")

        # TODO: implement controlled note sync mapping here if required.

    except Exception as e:
        logger.error(f"[NOTE SYNC] Error while processing note {note_id}: {e}", exc_info=True)
def handle_version_creation(entity, session_pbv, session_undark):
    """
    Syncs AssetVersion creation.
    CORRECTED: Uses the 'version' attribute (integer) instead of the non-existent 'name' attribute.
    """
    version_id = entity.get('entityId')
    if not version_id:
        logger.warning("[VERSION SYNC] Event is missing version_id. Skipping.")
        return

    logger.info(f"[VERSION SYNC] Processing event for version_id: {version_id}")

    # <<< CHANGE 1: Corrected the query to select 'version', not 'name'. >>>
    query_projection = (
        'select version, comment, status, user, '
        'asset.name, asset.project.name, asset.project.id '
        'from AssetVersion '
        f'where id is "{version_id}"'
    )

    source_session = None
    target_session = None
    source_name = None

    # Determine the source server of the event
    if session_pbv.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_pbv
        target_session = session_undark
        source_name = "PBV"
    elif session_undark.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_undark
        target_session = session_pbv
        source_name = "UNDARK"
    else:
        logger.error(f"[VERSION SYNC] Version ID {version_id} not found on either server.")
        return

    # Perform the query a single time
    source_version = source_session.query(query_projection).first()

    # The check should be just for the existence of the entity now
    if not source_version:
        logger.error(f"[VERSION SYNC] Failed to get version data from {source_name} for {version_id}. Skipping.")
        return

    # <<< CHANGE 2: Use the integer 'version' number, not a name. >>>
    project_name = source_version['asset']['project']['name']
    asset_name = source_version['asset']['name']
    version_number = source_version['version'] # This is an integer
    target_name = "UNDARK" if source_name == "PBV" else "PBV"

    logger.info(f"[VERSION SYNC] Source {source_name}: '{project_name}' > '{asset_name}' > v{version_number}")

    try:
        # Find the corresponding project and asset on the target server
        target_project = target_session.query(f'Project where name is "{project_name}"').first()
        if not target_project:
            logger.warning(f"[VERSION SYNC] Project '{project_name}' not found on target {target_name}. Skipping.")
            return

        target_asset = target_session.query(f'Asset where name is "{asset_name}" and project.id is "{target_project["id"]}"').first()
        if not target_asset:
            logger.warning(f"[VERSION SYNC] Asset '{asset_name}' not found on target {target_name}. Skipping.")
            return

        # <<< CHANGE 3: Check for existing version using the version NUMBER and asset ID. >>>
        # Note: No quotes around {version_number} because it's an integer comparison.
        existing_version = target_session.query(
            f'AssetVersion where version is {version_number} and asset.id is "{target_asset["id"]}"'
        ).first()
        if existing_version:
            logger.info(f"[VERSION SYNC] Version {version_number} for asset '{asset_name}' already exists on target {target_name}. Skipping.")
            return

        # <<< CHANGE 4: Create the new version using the 'version' attribute. >>>
        target_session.create('AssetVersion', {
            'version': version_number,
            'asset': target_asset,
            'status': source_version['status'],
            'comment': source_version['comment'],
            'user': source_version['user']
        })
        target_session.commit()
        logger.info(f"[VERSION SYNC] SUCCESS: Synced version {version_number} to {target_name}.")

    except Exception as e:
        logger.error(f"[VERSION SYNC] An unexpected error occurred during sync to {target_name}: {e}", exc_info=True)

def register(session_pbv):
    """Registers the event listeners for both sessions."""
    logger.info("Registering UNDARK-PBV Sync listeners...")
    session_undark = get_ftrack_session(UNDARK_FTRACK_API_KEY, UNDARK_FTRACK_API_USER, UNDARK_FTRACK_API_URL)

    # Create a single callback function with all sessions
    callback = functools.partial(sync_event_handler, session_pbv, session_undark)
    
    # Subscribe both hubs to the same callback
    session_pbv.event_hub.subscribe('topic=ftrack.update', callback)
    session_undark.event_hub.subscribe('topic=ftrack.update', callback)

    # **FIX**: Start the UNDARK listener in a separate thread
    undark_thread = threading.Thread(target=session_undark.event_hub.wait)
    undark_thread.daemon = True
    undark_thread.start()
    logger.info("UNDARK listener started in a separate thread.")

if __name__ == '__main__':
    logger.info("Starting UNDARK-PBV Sync standalone process...")
    session_pbv = get_ftrack_session(PBV_FTRACK_API_KEY, PBV_FTRACK_API_USER, PBV_FTRACK_API_URL)
    register(session_pbv)
    
    logger.info("Main thread waiting for PBV ftrack events...")
    # **FIX**: The main thread will now wait for PBV events, while the other thread waits for UNDARK events
    session_pbv.event_hub.wait()