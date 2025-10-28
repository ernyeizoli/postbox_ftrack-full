import os
import threading
import logging
import functools

from dotenv import load_dotenv
import ftrack_api

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
        auto_connect_event_hub=True,
    )


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
        logger.info(
            "Processing 'asset-request' task creation: '%s' in project '%s'",
            task['name'],
            project_name,
        )

        target_project = session_undark.query(
            f'Project where name is "{project_name}"'
        ).first()
        if not target_project:
            logger.warning(
                "Project '%s' not found in UNDARK. Cannot sync task.", project_name
            )
            return

        existing_task = session_undark.query(
            f'Task where name is "{task["name"]}" and parent.id is "{target_project["id"]}"'
        ).first()

        if not existing_task:
            session_undark.create('Task', {'name': task['name'], 'parent': target_project})
            session_undark.commit()
            logger.info("SUCCESS: Synced task '%s' to UNDARK.", task['name'])
        else:
            logger.info("Task '%s' already exists in UNDARK. Skipping.", task['name'])

    except Exception as exc:
        logger.error("Error processing task creation sync: %s", exc, exc_info=True)


def handle_note_creation(entity, session_pbv, session_undark):
    """Synchronize note creations between PBV and UNDARK."""

    note_id = entity.get('entityId')
    if not note_id:
        logger.warning("[NOTE SYNC] Event missing note id. Skipping.")
        return

    if entity.get('action') != 'add':
        logger.info(
            "[NOTE SYNC] Ignoring non-add action '%s' for note %s.",
            entity.get('action'),
            note_id,
        )
        return

    def _escape(value):
        if isinstance(value, str):
            return value.replace('"', '\\"')
        return value

    def _get(entity_obj, key, default=None):
        try:
            if hasattr(entity_obj, 'get'):
                return entity_obj.get(key, default)
            return entity_obj[key]
        except Exception:
            return default

    try:
        note_on_pbv = session_pbv.query(f'Note where id is "{note_id}"').first()
        note_on_undark = session_undark.query(f'Note where id is "{note_id}"').first()

        if note_on_pbv and note_on_undark:
            logger.info("[NOTE SYNC] Note %s already present on both servers. Skipping.", note_id)
            return
        elif note_on_pbv:
            source_session = session_pbv
            source_name = 'PBV'
            target_session = session_undark
            target_name = 'UNDARK'
        elif note_on_undark:
            source_session = session_undark
            source_name = 'UNDARK'
            target_session = session_pbv
            target_name = 'PBV'
        else:
            logger.warning(
                "[NOTE SYNC] Note ID %s not found on either server. Skipping.", note_id
            )
            return

        source_note = source_session.query(
            'select text, content, subject, category, isTodo, user, '
            'parent, parent.entity_type, parent.name, parent.project.name, '
            'parent.project.id '
            f'from Note where id is "{note_id}"'
        ).first()

        if not source_note:
            logger.warning(
                "[NOTE SYNC] Unable to load full data for note %s on %s. Skipping.",
                note_id,
                source_name,
            )
            return

        parent = _get(source_note, 'parent')
        if not parent:
            logger.warning(
                "[NOTE SYNC] Note %s on %s has no parent. Skipping.", note_id, source_name
            )
            return

        parent_type = _get(parent, 'entity_type') or _get(parent, 'type')
        if parent_type != 'Task':
            logger.info(
                "[NOTE SYNC] Note %s parent type '%s' not supported yet. Skipping.",
                note_id,
                parent_type,
            )
            return

        task_name = _get(parent, 'name')
        project = _get(parent, 'project') or {}
        project_name = _get(project, 'name')
        if not (task_name and project_name):
            logger.warning(
                "[NOTE SYNC] Note %s missing parent names (task/project). Skipping.",
                note_id,
            )
            return

        logger.info(
            "[NOTE SYNC] Syncing note %s from %s task '%s' / project '%s' -> %s.",
            note_id,
            source_name,
            task_name,
            project_name,
            target_name,
        )

        target_project = target_session.query(
            f'Project where name is "{_escape(project_name)}"'
        ).first()
        if not target_project:
            logger.warning(
                "[NOTE SYNC] Project '%s' not found on %s. Skipping note sync.",
                project_name,
                target_name,
            )
            return

        target_task = target_session.query(
            f'Task where name is "{_escape(task_name)}" and project.id is "{target_project["id"]}"'
        ).first()
        if not target_task:
            logger.warning(
                "[NOTE SYNC] Task '%s' not found on %s. Skipping note sync.",
                task_name,
                target_name,
            )
            return

        note_text = _get(source_note, 'text') or ''
        escaped_text = _escape(note_text)
        existing_target_note = target_session.query(
            f'Note where parent.id is "{target_task["id"]}" and text is "{escaped_text}"'
        ).first()
        if existing_target_note:
            logger.info(
                "[NOTE SYNC] Matching note already exists on %s. Skipping.", target_name
            )
            return

        note_payload = {
            'parent': target_task,
            'text': note_text,
            'content': _get(source_note, 'content') or note_text,
            'isTodo': _get(source_note, 'isTodo', False),
        }

        category = _get(source_note, 'category')
        cat_name = _get(category, 'name') if category else None
        if cat_name:
            cat_name = _escape(cat_name)
            target_category = target_session.query(
                f'NoteCategory where name is "{cat_name}"'
            ).first()
            if target_category:
                note_payload['category'] = target_category

        target_session.create('Note', note_payload)
        target_session.commit()
        logger.info(
            "[NOTE SYNC] SUCCESS: Synced note '%s' to %s.", note_text[:40], target_name
        )

    except Exception as exc:
        logger.error(
            "[NOTE SYNC] Error while processing note %s: %s",
            note_id,
            exc,
            exc_info=True,
        )


def handle_version_creation(entity, session_pbv, session_undark):
    """Sync AssetVersion creation between servers using the version number."""

    version_id = entity.get('entityId')
    if not version_id:
        logger.warning("[VERSION SYNC] Event is missing version_id. Skipping.")
        return

    logger.info("[VERSION SYNC] Processing event for version_id: %s", version_id)

    query_projection = (
        'select version, comment, status, user, '
        'asset.name, asset.project.name, asset.project.id '
        'from AssetVersion '
        f'where id is "{version_id}"'
    )

    if session_pbv.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_pbv
        target_session = session_undark
        source_name = 'PBV'
        target_name = 'UNDARK'
    elif session_undark.query(f'AssetVersion where id is "{version_id}"').first():
        source_session = session_undark
        target_session = session_pbv
        source_name = 'UNDARK'
        target_name = 'PBV'
    else:
        logger.error(
            "[VERSION SYNC] Version ID %s not found on either server.", version_id
        )
        return

    source_version = source_session.query(query_projection).first()
    if not source_version:
        logger.error(
            "[VERSION SYNC] Failed to get version data from %s for %s. Skipping.",
            source_name,
            version_id,
        )
        return

    project_name = source_version['asset']['project']['name']
    asset_name = source_version['asset']['name']
    version_number = source_version['version']

    logger.info(
        "[VERSION SYNC] Source %s: '%s' > '%s' > v%s",
        source_name,
        project_name,
        asset_name,
        version_number,
    )

    try:
        target_project = target_session.query(
            f'Project where name is "{project_name}"'
        ).first()
        if not target_project:
            logger.warning(
                "[VERSION SYNC] Project '%s' not found on target %s. Skipping.",
                project_name,
                target_name,
            )
            return

        target_asset = target_session.query(
            f'Asset where name is "{asset_name}" and project.id is "{target_project["id"]}"'
        ).first()
        if not target_asset:
            logger.warning(
                "[VERSION SYNC] Asset '%s' not found on target %s. Skipping.",
                asset_name,
                target_name,
            )
            return

        existing_version = target_session.query(
            f'AssetVersion where version is {version_number} and asset.id is "{target_asset["id"]}"'
        ).first()
        if existing_version:
            logger.info(
                "[VERSION SYNC] Version %s for asset '%s' already exists on %s. Skipping.",
                version_number,
                asset_name,
                target_name,
            )
            return

        target_session.create(
            'AssetVersion',
            {
                'version': version_number,
                'asset': target_asset,
                'status': source_version['status'],
                'comment': source_version['comment'],
                'user': source_version['user'],
            },
        )
        target_session.commit()
        logger.info(
            "[VERSION SYNC] SUCCESS: Synced version %s to %s.",
            version_number,
            target_name,
        )

    except Exception as exc:
        logger.error(
            "[VERSION SYNC] Unexpected error during sync to %s: %s",
            target_name,
            exc,
            exc_info=True,
        )


def sync_event_handler(session_pbv, session_undark, event):
    """Callback function to handle ftrack events for synchronization."""
    logger.info("Event received.")
    for entity in event['data'].get('entities', []):
        action = entity.get('action')
        entity_type = (entity.get('entity_type') or '').lower()

        if entity_type == 'task' and action == 'add':
            handle_task_creation(entity, session_pbv, session_undark)
        elif entity_type == 'note' and action in {'add', 'update'}:
            handle_note_creation(entity, session_pbv, session_undark)
        elif entity_type == 'assetversion' and action == 'add':
            handle_version_creation(entity, session_pbv, session_undark)


def register(session_pbv):
    """Registers the event listeners for both sessions."""
    logger.info("Registering UNDARK-PBV Sync listeners...")
    session_undark = get_ftrack_session(
        UNDARK_FTRACK_API_KEY,
        UNDARK_FTRACK_API_USER,
        UNDARK_FTRACK_API_URL,
    )

    callback = functools.partial(sync_event_handler, session_pbv, session_undark)

    session_pbv.event_hub.subscribe('topic=ftrack.update', callback)
    session_undark.event_hub.subscribe('topic=ftrack.update', callback)

    undark_thread = threading.Thread(target=session_undark.event_hub.wait)
    undark_thread.daemon = True
    undark_thread.start()
    logger.info("UNDARK listener started in a separate thread.")


if __name__ == '__main__':
    logger.info("Starting UNDARK-PBV Sync standalone process...")
    session_pbv = get_ftrack_session(
        PBV_FTRACK_API_KEY,
        PBV_FTRACK_API_USER,
        PBV_FTRACK_API_URL,
    )
    register(session_pbv)

    logger.info("Main thread waiting for PBV ftrack events...")
    session_pbv.event_hub.wait()