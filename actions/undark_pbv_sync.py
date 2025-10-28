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
    """Synchronize note creations between PBV and UNDARK.

    Current behaviour:
    * Only handles `add` events (new notes).
    * Supports notes whose parent is a Task.
    * Avoids duplicate creation by checking existing notes on the target task.
    """

    note_id = entity.get('entityId')
    if not note_id:
        logger.warning("[NOTE SYNC] Event missing note id. Skipping.")
        return

    if entity.get('action') != 'add':
        logger.info(f"[NOTE SYNC] Ignoring non-add action '{entity.get('action')}' for note {note_id}.")
        return

    def _escape(value: str) -> str:
        return value.replace('"', '\"') if isinstance(value, str) else value

    def _get(entity, key, default=None):
        try:
            if hasattr(entity, 'get'):
                return entity.get(key, default)
            return entity[key]
        except Exception:
            return default

    try:
        # Determine source server where the note exists.
        note_on_pbv = session_pbv.query(f'Note where id is "{note_id}"').first()
        note_on_undark = session_undark.query(f'Note where id is "{note_id}"').first()

        if note_on_pbv and note_on_undark:
            logger.info(f"[NOTE SYNC] Note {note_id} already present on both servers. Skipping.")
            return
        elif note_on_pbv:
            source_session = session_pbv
            source_name = 'PBV'
            source_note = note_on_pbv
            target_session = session_undark
            target_name = 'UNDARK'
        elif note_on_undark:
            source_session = session_undark
            source_name = 'UNDARK'
            source_note = note_on_undark
            target_session = session_pbv
            target_name = 'PBV'
        else:
            logger.warning(f"[NOTE SYNC] Note ID {note_id} not found on either server. Skipping.")
            return

        # Fetch richer details about the note and its parent.
        source_note = source_session.query(
            'select text, content, subject, category, isTodo, user, '
            'parent, parent.entity_type, parent.name, parent.project.name, '
            'parent.project.id '
            f'from Note where id is "{note_id}"'
        ).first()

        if not source_note:
            logger.warning(f"[NOTE SYNC] Unable to load full data for note {note_id} on {source_name}. Skipping.")
            return

        parent = _get(source_note, 'parent')
        if not parent:
            logger.warning(f"[NOTE SYNC] Note {note_id} on {source_name} has no parent. Skipping.")
            return

        parent_type = _get(parent, 'entity_type') or _get(parent, 'type')
        if parent_type != 'Task':
            logger.info(f"[NOTE SYNC] Note {note_id} parent type '{parent_type}' not supported yet. Skipping.")
            return

        task_name = _get(parent, 'name')
        project = _get(parent, 'project') or {}
        project_name = _get(project, 'name')
        if not (task_name and project_name):
            logger.warning(f"[NOTE SYNC] Note {note_id} missing parent names (task/project). Skipping.")
            return

        logger.info(f"[NOTE SYNC] Syncing note {note_id} from {source_name} task '{task_name}' / project '{project_name}' -> {target_name}.")

        target_project = target_session.query(f'Project where name is "{_escape(project_name)}"').first()
        if not target_project:
            logger.warning(f"[NOTE SYNC] Project '{project_name}' not found on {target_name}. Skipping note sync.")
            return

        target_task = target_session.query(
            f'Task where name is "{_escape(task_name)}" and project.id is "{target_project["id"]}"'
        ).first()
        if not target_task:
            logger.warning(f"[NOTE SYNC] Task '{task_name}' not found on {target_name}. Skipping note sync.")
            return

        note_text = source_note.get('text') or ''
        escaped_text = _escape(note_text)
        existing_target_note = target_session.query(
            f'Note where parent.id is "{target_task["id"]}" and text is "{escaped_text}"'
        ).first()
        if existing_target_note:
            logger.info(f"[NOTE SYNC] Matching note already exists on {target_name}. Skipping.")
            return

        note_payload = {
            'parent': target_task,
            'text': note_text,
            'content': _get(source_note, 'content') or note_text,
            'isTodo': _get(source_note, 'isTodo', False),
        }

        # Copy category if available and resolvable by name.
        category = _get(source_note, 'category')
        cat_name = _get(category, 'name') if category else None
        if cat_name:
            cat_name = _escape(cat_name)
            target_category = target_session.query(f'NoteCategory where name is "{cat_name}"').first()
            if target_category:
                note_payload['category'] = target_category

        target_session.create('Note', note_payload)
        target_session.commit()
        logger.info(f"[NOTE SYNC] SUCCESS: Synced note '{note_text[:40]}' to {target_name}.")

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