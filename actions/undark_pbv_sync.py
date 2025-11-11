"""
UNDARK ↔ PBV Ftrack Sync Tool
----------------------------------
Synchronizes Tasks, Notes, and AssetVersions between two ftrack servers.
Enhanced with detailed logging and defensive error handling.
"""

import os
import threading
import logging
import functools
import time
from dotenv import load_dotenv
import ftrack_api


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("undark_pbv_sync")


# Tracks notes we just mirrored so their follow-up events are ignored.
SYNCED_NOTE_IDS = set()
SYNCED_NOTE_IDS_LOCK = threading.Lock()


# --- Load Environment ---
load_dotenv()

PBV_FTRACK_API_KEY = os.getenv("FTRACK_API_KEY")
PBV_FTRACK_API_USER = os.getenv("FTRACK_API_USER")
PBV_FTRACK_API_URL = os.getenv("FTRACK_SERVER")

UNDARK_FTRACK_API_KEY = os.getenv("UNDARK_FTRACK_API_KEY")
UNDARK_FTRACK_API_USER = os.getenv("UNDARK_FTRACK_API_USER")
UNDARK_FTRACK_API_URL = os.getenv("UNDARK_FTRACK_API_URL")


# --- Helper Functions ---
def get_ftrack_session(api_key, api_user, api_url):
    logger.info("Connecting to ftrack server: %s as %s", api_url, api_user)
    try:
        session = ftrack_api.Session(
            api_key=api_key,
            api_user=api_user,
            server_url=api_url,
            auto_connect_event_hub=True,
        )
        logger.info("Connected successfully to %s", api_url)
        return session
    except Exception as e:
        logger.critical("Failed to connect to %s: %s", api_url, e)
        raise


def _escape(value):
    if isinstance(value, str):
        return value.replace('"', '\\"')
    return value


def _get(entity, key, default=None):
    try:
        if hasattr(entity, "get"):
            return entity.get(key, default)
        return entity[key]
    except Exception:
        return default


def _safe_str(value):
    try:
        return str(value)
    except Exception:
        return "<unprintable>"


def _resolve_entity_type(entity):
    return (entity.get("entity_type") or entity.get("entityType") or "").lower()


def _resolve_action(entity):
    return (entity.get("action") or entity.get("operation") or "").lower()


def _resolve_note_id(entity):
    return entity.get("entityId") or entity.get("id")


def _describe_version_location(version):
    """Return a human-readable hierarchy path for an asset version."""
    try:
        asset = _get(version, "asset")
        version_name = _get(version, "name") or (
            f"v{_get(version, 'version')}" if _get(version, "version") is not None else None
        )

        project_name = None
        asset_name = None
        task_name = None

        if asset:
            asset_name = _get(asset, "name")
            project = _get(asset, "project")
            project_name = _get(project, "name") if project else None

        task = _get(version, "task")
        task_name = _get(task, "name") if task else None

        parts = [part for part in (project_name, task_name, asset_name, version_name) if part]
        return " / ".join(parts) if parts else "<unknown>"
    except Exception as exc:
        return f"<unknown: {_safe_str(exc)}>"


def _event_hub_listener(event_hub, label, retry_delay=30):
    """Continuously wait on an event hub, reconnecting after transient failures."""
    while True:
        try:
            logger.info("[%s EVENT HUB] Waiting for events...", label)
            event_hub.wait()
            logger.info("[%s EVENT HUB] wait() exited; restarting listener.", label)
        except ftrack_api.exception.EventHubConnectionError as exc:
            logger.error(
                "[%s EVENT HUB] Connection error: %s. Retrying in %s seconds.",
                label,
                _safe_str(exc),
                retry_delay,
            )
            time.sleep(retry_delay)
        except Exception as exc:
            logger.exception(
                "[%s EVENT HUB] Unexpected error: %s. Retrying in %s seconds.",
                label,
                _safe_str(exc),
                retry_delay,
            )
            time.sleep(retry_delay)


# --- Task Sync ---
def handle_task_creation(entity, session_pbv, session_undark):
    task_id = entity.get("entityId")
    if not task_id:
        return

    try:
        logger.info("[TASK SYNC] Checking for new task %s on PBV...", task_id)
        task = session_pbv.get("Task", task_id)
        if not task:
            logger.warning("[TASK SYNC] Task %s not found on PBV.", task_id)
            return

        name = task["name"]
        if "asset-request" not in name.lower():
            logger.debug("[TASK SYNC] Task %s is not an 'asset-request'; skipping.", name)
            return

        project_name = task["project"]["name"]
        logger.info("[TASK SYNC] Syncing '%s' in project '%s'...", name, project_name)

        target_project = session_undark.query(
            f'Project where name is "{_escape(project_name)}"'
        ).first()
        if not target_project:
            logger.warning("[TASK SYNC] Target project not found on UNDARK: %s", project_name)
            return

        existing = session_undark.query(
            f'Task where name is "{_escape(name)}" and parent.id is "{target_project["id"]}"'
        ).first()
        if existing:
            logger.info("[TASK SYNC] Task '%s' already exists on UNDARK.", name)
            return

        new_task = session_undark.create("Task", {"name": name, "parent": target_project})
        session_undark.commit()
        logger.info("[TASK SYNC] Created task '%s' (id=%s) on UNDARK.", name, new_task["id"])

    except Exception as e:
        logger.exception("[TASK SYNC] Error syncing task: %s", e)


# --- Note Sync ---
def handle_note_creation(entity, session_pbv, session_undark):
    """
    Handles syncing a new note from one server to another.
    Minimal version with basic logging and simple exception handling.
    """
    
    # --- 1. Initial Check ---
    note_id = _resolve_note_id(entity)
    action = _resolve_action(entity)
    
    # Silently skip if this is not a new note event
    if not note_id or action != "add":
        return 

    with SYNCED_NOTE_IDS_LOCK:
        if note_id in SYNCED_NOTE_IDS:
            SYNCED_NOTE_IDS.remove(note_id)
            logger.debug("[NOTE SYNC] Skipping note %s; already mirrored.", note_id)
            return

    logger.info(f"[NOTE SYNC] Starting sync for new note: {note_id}")

    try:
        # --- 2. Determine Source & Target ---
        note_pbv = session_pbv.get("Note", note_id)
        note_undark = session_undark.get("Note", note_id)

        # Silently skip if already synced or if the note wasn't found
        if (note_pbv and note_undark) or (not note_pbv and not note_undark):
            return

        source, target = (session_pbv, session_undark) if note_pbv else (session_undark, session_pbv)
        source_name, target_name = ("PBV", "UNDARK") if note_pbv else ("UNDARK", "PBV")
        source_note = note_pbv or note_undark

        # --- 3. Find Source Parent Info ---
        changes = entity.get("changes", {})
        parent_id = entity.get("parentId") or _get(changes.get("parent_id", {}), "new")
        parent_type = entity.get("parent_type") or _get(changes.get("parent_type", {}), "new")

        if not parent_id or not parent_type:
            return # Silently skip if no parent info

        # Map 'show' to 'Project'
        parent_entity_type = "Project" if parent_type == "show" else parent_type.capitalize()
        parent = source.query(f'{parent_entity_type} where id is "{parent_id}"').first()

        if not parent:
            return # Silently skip if parent not found

        # --- 4. Get Project/Task Names from Source ---
        try:
            source.populate(parent, "project")
            project_name = _get(_get(parent, "project"), "name")
        except Exception:
            project_name = None
        task_name = _get(parent, "name")

        # This is a critical failure, so we log it before returning
        if not project_name or not task_name:
            logger.error(f"[NOTE SYNC] Could not find project/task name for parent {parent_id}. Skipping.")
            return

        # --- 5. Find Target Project & Task ---
        target_project = target.query(f'Project where name is "{_escape(project_name)}"').first()
        if not target_project:
            return # Silently skip if target project not found

        target_task = target.query(
            f'Task where name is "{_escape(task_name)}" and project.id is "{target_project["id"]}"'
        ).first()
        if not target_task:
            return # Silently skip if target task not found

# --- 6. Build Note payload & Create Note ---
        
        # Get content and subject from the source note
        content = _get(source_note, "content") or _get(source_note, "text") or ""
        subject = _get(source_note, "subject") or ""
        author = _get(source_note, "user") or _get(source_note, "author")

        # --- FIX as per documentation ---
        # The create_note() method does not accept a 'subject'.
        # We will prepend the subject to the main content.
        
        final_content = content
        if subject:
            # Use markdown for a clear "Subject:" header
            final_content = f"**Subject:** {subject}\n\n{content}"

        # Ensure we don't pass an empty string if both were empty
        if not final_content.strip():
            final_content = "No content"
        # -------------------------------

        # Map author
        target_author = None
        if author:
            username = _get(author, "username") or _get(author, "name")
            if username:
                target_author = target.query(f'User where username is "{_escape(username)}"').first()

        recipients = [target_author] if target_author else []

        # --- ✅ Create the note using the documented API method ---
        # (Note: 'subject=subject' has been removed)
        logger.debug("[NOTE SYC] Creating note on %s via create_note()", target_name)
        target_note = target_task.create_note(
            final_content,  # Pass the new combined content
            author=target_author,
            recipients=recipients,
        )
        with SYNCED_NOTE_IDS_LOCK:
            SYNCED_NOTE_IDS.add(target_note["id"])
        target.commit()
        logger.info("[NOTE SYC] SUCCESS: Synced note %s to %s (id=%s).",
                    note_id, target_name, target_note["id"])

    except Exception as e:
        # Simple, broad exception handling
        logger.error(f"[NOTE SYNC] FAILED to sync note {note_id}: {e}", exc_info=True)
        logger.exception("[NOTE SYNC] Failed to sync note %s: %s", note_id, e)

# --- Version Sync ---
def handle_version_creation(entity, session_pbv, session_undark):
    version_id = entity.get("entityId")
    if not version_id:
        return

    logger.info("[VERSION SYNC] Version ID: %s", version_id)

    pbv_ver = session_pbv.query(f'AssetVersion where id is "{version_id}"').first()
    undark_ver = session_undark.query(f'AssetVersion where id is "{version_id}"').first()

    if not pbv_ver and not undark_ver:
        logger.warning("[VERSION SYNC] Version %s not found anywhere.", version_id)
        return

    source, target = (session_pbv, session_undark) if pbv_ver else (session_undark, session_pbv)
    src_name, tgt_name = ("PBV", "UNDARK") if pbv_ver else ("UNDARK", "PBV")
    version = pbv_ver or undark_ver

    asset = version["asset"]
    project_name = asset["project"]["name"]
    asset_name = asset["name"]
    version_name = _get(version, "name")
    version_number = _get(version, "version")
    comment = _get(version, "comment")

    raw_metadata = _get(version, "metadata") or {}
    metadata = dict(raw_metadata) if hasattr(raw_metadata, "items") else {}
    metadata_product = metadata.get("product")
    metadata_product_path = metadata.get("productpath")
    if metadata_product:
        logger.debug(
            "[VERSION SYNC] Metadata product detected for version %s: %s",
            version_id,
            metadata_product,
        )
    if metadata_product_path:
        logger.debug(
            "[VERSION SYNC] Metadata productpath for version %s: %s",
            version_id,
            metadata_product_path,
        )

    # Retrieve the task associated with the version so we can mirror its parent relationship.
    source_task = None
    try:
        if "task" not in version.keys():
            source.populate(version, "task")
        source_task = _get(version, "task")
    except Exception as exc:
        logger.debug("[VERSION SYNC] Failed to populate task for version %s: %s", version_id, _safe_str(exc))

    if not version_name and version_number is not None:
        version_name = f"v{version_number}"

    logger.info("[VERSION SYNC] %s -> %s: %s / %s / %s", src_name, tgt_name, project_name, asset_name, version_name or version_id)

    tgt_project = target.query(f'Project where name is "{_escape(project_name)}"').first()
    if not tgt_project:
        logger.warning("[VERSION SYNC] Project not found on %s: %s", tgt_name, project_name)
        return

    # Attempt to find the matching task on the target side before we need it for asset or version creation.
    task_name = _get(source_task, "name") if source_task else None
    if not task_name and metadata_product:
        task_name = metadata_product
        logger.info(
            "[VERSION SYNC] Using metadata product '%s' as task name for version %s.",
            metadata_product,
            version_id,
        )

    target_task = None
    task_payload = None
    task_parent_hint = None
    target_task_parent = None
    task_type_target = None
    if source_task:
        if task_name:
            logger.debug(
                "[VERSION SYNC] Source version %s belongs to task '%s'. Looking for match on %s.",
                version_id,
                task_name,
                tgt_name,
            )
            target_task = target.query(
                f'Task where name is "{_escape(task_name)}" and project.id is "{tgt_project["id"]}"'
            ).first()
            if not target_task:
                logger.warning(
                    "[VERSION SYNC] Target task not found on %s for task '%s'. Falling back to project parent.",
                    tgt_name,
                    task_name,
                )
            else:
                logger.info(
                    "[VERSION SYNC] Found matching task '%s' on %s (id=%s).",
                    task_name,
                    tgt_name,
                    target_task["id"],
                )

        source_task_type = _get(source_task, "type")
        task_type_name = _get(source_task_type, "name") if source_task_type else None
        if task_type_name:
            try:
                task_type_target = target.query(
                    f'TaskType where name is "{_escape(task_type_name)}"'
                ).first()
            except KeyError:
                task_type_target = None
                logger.debug(
                    "[VERSION SYNC] TaskType not available on %s; skipping type mapping.",
                    tgt_name,
                )
            if task_type_target:
                logger.debug(
                    "[VERSION SYNC] Matched task type '%s' on %s.",
                    task_type_name,
                    tgt_name,
                )

    if not target_task and task_name and not source_task:
        target_task = target.query(
            f'Task where name is "{_escape(task_name)}" and project.id is "{tgt_project["id"]}"'
        ).first()
        if target_task:
            logger.info(
                "[VERSION SYNC] Found task '%s' on %s via metadata lookup (id=%s).",
                task_name,
                tgt_name,
                target_task["id"],
            )

    if source_task and task_name and not target_task:
        try:
            if "parent" not in source_task.keys():
                source.populate(source_task, "parent")
        except Exception as exc:
            logger.debug(
                "[VERSION SYNC] Failed to populate task parent for %s: %s",
                task_name,
                _safe_str(exc),
            )

        source_task_parent = _get(source_task, "parent")
        parent_id = _get(source_task_parent, "id")
        parent_type = (_get(source_task_parent, "entity_type") or "").lower()

        if parent_id and parent_id == _get(asset, "id"):
            task_parent_hint = "asset"
        elif parent_type == "project":
            target_task_parent = tgt_project
        elif parent_type == "task":
            parent_name = _get(source_task_parent, "name")
            if parent_name:
                target_task_parent = target.query(
                    f'Task where name is "{_escape(parent_name)}" and project.id is "{tgt_project["id"]}"'
                ).first()

        if not target_task_parent:
            logger.warning(
                "[VERSION SYNC] Could not determine parent for task '%s' on %s. Using project root.",
                task_name,
                tgt_name,
            )
            target_task_parent = tgt_project

        if target_task_parent:
            task_payload = {"name": task_name, "parent": target_task_parent}

    tgt_asset = target.query(
        f'Asset where name is "{_escape(asset_name)}" and project.id is "{tgt_project["id"]}"'
    ).first()
    if not tgt_asset:
        logger.warning("[VERSION SYNC] Asset not found on %s: %s", tgt_name, asset_name)
        # Attempt to mirror the asset structure on the target server.
        tgt_asset_payload = {"name": asset_name, "parent": tgt_project}

        asset_type_info = _get(asset, "type") or _get(asset, "asset_type")
        asset_type_name = _get(asset_type_info, "name")
        if asset_type_name:
            logger.debug("[VERSION SYNC] Looking up AssetType '%s' on %s", asset_type_name, tgt_name)
            asset_type_target = target.query(
                f'AssetType where name is "{_escape(asset_type_name)}"'
            ).first()
            if asset_type_target:
                tgt_asset_payload["type"] = asset_type_target

        logger.info("[VERSION SYNC] Creating asset '%s' on %s", asset_name, tgt_name)
        tgt_asset = target.create("Asset", tgt_asset_payload)
        logger.debug(
            "[VERSION SYNC] Newly created asset '%s' attached to project '%s' on %s.",
            asset_name,
            project_name,
            tgt_name,
        )

    if task_parent_hint == "asset" and not target_task_parent:
        target_task_parent = tgt_asset

    if not target_task_parent and task_name:
        target_task_parent = tgt_project

    if not target_task and task_name and not task_payload:
        task_payload = {"name": task_name, "parent": target_task_parent}
        if task_type_target:
            task_payload["type"] = task_type_target

    tgt_asset_id = tgt_asset["id"]

    exists_query = None
    if version_number is not None:
        exists_query = f'AssetVersion where version is {version_number} and asset.id is "{tgt_asset_id}"'
    elif version_name:
        exists_query = f'AssetVersion where name is "{_escape(version_name)}" and asset.id is "{tgt_asset_id}"'

    exists = target.query(exists_query).first() if exists_query else None
    if exists:
        try:
            target.populate(exists, "task")
            target.populate(exists, "asset")
            asset_for_exists = _get(exists, "asset")
            if asset_for_exists:
                target.populate(asset_for_exists, "project")
        except Exception as exc:
            logger.debug(
                "[VERSION SYNC] Failed to populate existing version %s context: %s",
                _get(exists, "id"),
                _safe_str(exc),
            )

        location = _describe_version_location(exists)
        logger.info(
            "[VERSION SYNC] Version already exists on %s at %s (id=%s)",
            tgt_name,
            location,
            _get(exists, "id"),
        )
        return

    if not target_task and task_payload:
        logger.info(
            "[VERSION SYNC] Creating task '%s' on %s under parent '%s'.",
            task_name,
            tgt_name,
            _get(task_payload.get("parent"), "name"),
        )
        target_task = target.create("Task", task_payload)

    payload = {"asset": tgt_asset}
    if version_name:
        payload["name"] = version_name
    if version_number is not None:
        payload["version"] = version_number
    if comment:
        payload["comment"] = comment
    if metadata:
        payload["metadata"] = metadata
    if target_task:
        payload["task"] = target_task
        logger.debug(
            "[VERSION SYNC] Associating new version with task '%s' on %s.",
            task_name,
            tgt_name,
        )

    if metadata_product or metadata_product_path:
        logger.debug(
            "[VERSION SYNC] Syncing metadata for version %s: product=%s path=%s",
            version_id,
            metadata_product,
            metadata_product_path,
        )

    target.create("AssetVersion", payload)
    target.commit()
    if target_task:
        logger.info(
            "[VERSION SYNC] SUCCESS: Created %s on %s under task '%s'.",
            version_name,
            tgt_name,
            task_name,
        )
    else:
        logger.info(
            "[VERSION SYNC] SUCCESS: Created %s on %s under project '%s'.",
            version_name,
            tgt_name,
            project_name,
        )


# --- Event Dispatcher ---
def sync_event_handler(session_pbv, session_undark, event):
    logger.debug("[EVENT] Raw event data: %s", event)
    for entity in event["data"].get("entities", []):
        action = _resolve_action(entity)
        etype = _resolve_entity_type(entity)
        logger.debug("[EVENT] Entity=%s Action=%s", etype, action)

        if etype == "task" and action == "add":
            handle_task_creation(entity, session_pbv, session_undark)
        elif etype == "note" and action == "add":
            handle_note_creation(entity, session_pbv, session_undark)
        elif etype == "assetversion" and action == "add":
            handle_version_creation(entity, session_pbv, session_undark)


# --- Registration ---
def register(session_pbv):
    logger.info("Registering event listeners...")
    session_undark = get_ftrack_session(
        UNDARK_FTRACK_API_KEY, UNDARK_FTRACK_API_USER, UNDARK_FTRACK_API_URL
    )

    callback = functools.partial(sync_event_handler, session_pbv, session_undark)
    topics = ["ftrack.update", "ftrack.note"]

    for topic in topics:
        session_pbv.event_hub.subscribe(f"topic={topic}", callback)
        session_undark.event_hub.subscribe(f"topic={topic}", callback)
        logger.info("Subscribed to topic: %s", topic)

    # Background listener for UNDARK
    thread = threading.Thread(
        target=_event_hub_listener,
        args=(session_undark.event_hub, "UNDARK"),
        daemon=True,
    )
    thread.start()
    logger.info("UNDARK listener thread started.")


# --- Main ---
if __name__ == "__main__":
    logger.info("Starting UNDARK-PBV Sync Service...")
    pbv = get_ftrack_session(PBV_FTRACK_API_KEY, PBV_FTRACK_API_USER, PBV_FTRACK_API_URL)
    register(pbv)
    logger.info("Listening for PBV events...")
    _event_hub_listener(pbv.event_hub, "PBV")
