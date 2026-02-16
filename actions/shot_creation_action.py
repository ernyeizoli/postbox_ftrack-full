import ftrack_api
import os
import logging
from dotenv import load_dotenv
import functools # Required to pass the session correctly
import time
from actions.copy_lock import is_copy_in_progress 

# --- Configuration ---
# Loads credentials from your .env file
load_dotenv()

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tasks_for_new_shot(session, event):
    """
    Listens for ftrack.update events and creates default tasks
    when a new 'Shot' type is created.
    This version is idempotent and uses batch commits.
    """
    # Skip processing if a project copy is in progress
    if is_copy_in_progress():
        logger.debug("--- Skipping event: Project copy in progress ---")
        return
    
    logger.info("--- Event received, processing entities... ---")
    
    for entity in event['data'].get('entities', []):
        logger.info(f"Inspecting entity: {entity.get('entity_type')} with action: {entity.get('action')}")

        if (entity.get('action') == 'add' and 
            entity.get('entity_type') == 'Shot'):

            shot_id = entity.get('entityId')
            if not shot_id:
                logger.warning("Found a new Shot entity but it had no ID. Skipping.")
                continue

            logger.info(f"MATCH! New Shot detected with ID: {shot_id}. Fetching details...")
            
            shot_object = None
            # Retry logic to handle potential database commit delays
            for i in range(5):
                shot_object = session.get('Shot', shot_id)
                if shot_object:
                    logger.info(f"Successfully fetched ftrack object on attempt {i+1}. Object Name: '{shot_object['name']}'")
                    break
                logger.warning(f"Attempt {i+1}: Shot object not found yet. Retrying in 1 second...")
                time.sleep(1)

            if not shot_object:
                logger.error(f"Failed to fetch details for shot {shot_id} after multiple attempts. Aborting for this entity.")
                continue

            try:
                project = shot_object['project']
                logger.info(f"Found parent project: '{project['full_name']}'")
                
                # --- Define your task template here ---
                task_names = ['Animation', 'Compositing']
                

                status = session.query('Status where name is "Not Started"').first()
                priority = session.query('Priority where name is "None"').first()

                if not status or status.get('entity_type') != 'Task':
                    logger.warning("Could not find a valid Task Status 'Not Started'. Tasks will be created with the default status.")
                    status = None
                if not priority:
                    logger.warning("Could not find Priority 'None'. Tasks will be created with the default priority.")

                logger.info(f"--- Starting Task Creation for Shot: '{shot_object['name']}' (ID: {shot_id}) ---")

                tasks_created_count = 0
                for task_name in task_names:
                    # Idempotency check
                    existing_task = session.query(
                        f'Task where name is "{task_name}" and parent.id is "{shot_id}"'
                    ).first()
                    if existing_task:
                        logger.info(f"Task '{task_name}' already exists. Skipping.")
                        continue

                    task_type = session.query(f'Type where name is "{task_name}"').first()
                    if not task_type:
                        logger.warning(f"Could not find a Task Type named '{task_name}'. Skipping creation of this task.")
                        continue

                    logger.info(f"Preparing to create Task '{task_name}' with Type '{task_type['name']}'...")
                    task_data = {
                        'name': task_name,
                        'parent': shot_object,
                        'type': task_type
                    }
                    if priority:
                        task_data['priority'] = priority
                    if status:
                        task_data['status'] = status
                    session.create('Task', task_data)
                    tasks_created_count += 1

                # Commit all prepared tasks in a single transaction outside the loop.
                if tasks_created_count > 0:
                    session.commit()
                    logger.info(f"SUCCESS! Committed {tasks_created_count} new tasks for shot '{shot_object['name']}'.")
                else:
                    logger.info("No new tasks were created (they may have all existed already).")

                logger.info("--- Finished Task Creation ---")

            except Exception as e:
                # Rollback any changes in the batch if an error occurs
                session.rollback()
                logger.exception(f"CRITICAL: An error occurred while processing shot ID {shot_id}. Transaction rolled back. Error: {e}")



def register_event_listener(session):
    """Registers the event listener with the ftrack session."""
    
    callback_with_session = functools.partial(create_tasks_for_new_shot, session)
    
    session.event_hub.subscribe(
        'topic=ftrack.update', 
        callback_with_session
    )
    
    logger.info("Event listener registered. Waiting for new Shots via ftrack.update...")
    session.event_hub.wait()



def register(session):
    """Register the shot creation automation."""
    logger.info("Registering Shot Creation Automation...")
    callback_with_session = functools.partial(create_tasks_for_new_shot, session)
    session.event_hub.subscribe(
        'topic=ftrack.update',
        callback_with_session
    )
    logger.info("Shot Creation Automation registered.")