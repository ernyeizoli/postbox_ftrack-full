import ftrack_api
import logging
import sys
import json
import datetime
import os
from dotenv import load_dotenv

import logging
logger = logging.getLogger(__name__)

class CreateProjectFromCopyAction:
    """Action to create a new project by copying an existing one."""

    label = 'Create Project from Copy'
    identifier = 'com.ftrack.create-from-copy.action'
    description = 'Creates a new project by copying an existing project structure.'

    def __init__(self, session):
        """Initialise action with ftrack session."""
        self.session = session
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )

    def register(self):
        """Register the action with the ftrack event hub."""
        self.session.event_hub.subscribe(
            'topic=ftrack.action.discover',
            self._discover
        )
        self.session.event_hub.subscribe(
            f'topic=ftrack.action.launch and data.actionIdentifier={self.identifier}',
            self._launch
        )
        self.logger.info(f'"{self.label}" action registered.')

    def _discover(self, event):
        """This action is global, so it should always be available."""
        self.logger.info("Discover event received, action is available.")
        return {
            'items': [{
                'label': self.label,
                'actionIdentifier': self.identifier,
                'icon': 'https://cdn.jsdelivr.net/npm/feather-icons/dist/icons/copy.svg'

            }]
        }

    def _launch(self, event):
        """Handles both displaying the form and processing the submission."""
        self.logger.info("Launch event received.")
        if 'values' in event['data']:
            self.logger.info("Processing form submission.")
            self._process_form(event)
            return {'success': True, 'message': 'Project copy job started!'}

        self.logger.info("Building form for user.")
        return self._build_form(event)

    def _build_form(self, event):
        """Queries for projects and returns the UI form definition."""
        self.logger.info("Querying for all projects to populate form dropdown.")
        
        all_projects = list(self.session.query('select id, full_name from Project'))
        self.logger.info(f"Found {len(all_projects)} projects.")

        if not all_projects:
            self.logger.warning("No projects found in ftrack instance.")
            return {
                'success': False,
                'message': 'No projects found to copy from.'
            }

        project_options = [
            {'label': p['full_name'], 'value': p['id']}
            for p in sorted(all_projects, key=lambda x: x['full_name'])
        ]

        return {
            'type': 'form',
            'title': 'Create Project from Copy',
            'submit_button_label': 'Create',
            'items': [
                {'type': 'label', 'value': '## Create Project from a Copy'},
                {'type': 'label', 'value': 'Select a source project and enter details for the new copy.'},
                {'type': 'hidden', 'name': 'user_id', 'value': event['source']['user']['id']},
                {
                    'label': 'Select Source Project',
                    'type': 'enumerator',
                    'name': 'source_project_id',
                    'data': project_options,
                    'value': project_options[0]['value']
                },
                {
                    'label': 'New Project Name',
                    'type': 'text',
                    'name': 'new_project_name',
                    'value': ''
                },
                {
                    'label': 'Start Date',
                    'type': 'date',
                    'name': 'new_start_date',
                    'value': datetime.date.today().isoformat()
                }
            ],
        }


    def _process_form(self, event):
        """Process form submission and start the background cloning job."""
        values = event['data']['values']
        self.logger.info(f"Form values received: {values}")

        user_id = values.pop('user_id')

        if not values.get('new_project_name'):
            self.logger.warning("Form submitted with no project name.")
            return {'success': False, 'message': 'Please enter a project name.'}
        
        job = self.session.create('Job', {
            'user_id': user_id,
            'status': 'running',
            'data': json.dumps({'description': f"Starting copy of project '{values['new_project_name']}'."})
        })
        self.session.commit()
        self.logger.info(f"Created job {job['id']} to track progress.")
        
        try:
            self._clone_project(values, job)
            job['data'] = json.dumps({'description': f"Successfully copied project '{values['new_project_name']}'."})
            job['status'] = 'done'
            self.logger.info(f"Job {job['id']} completed successfully.")
        except Exception as e:
            self.logger.error(f"Job {job['id']} failed: {e}", exc_info=True)
            job['data'] = json.dumps({'description': f"ERROR: Could not copy project. Reason: {e}"})
            job['status'] = 'failed'
        finally:
            self.session.commit()

    def _clone_project(self, form_data, job):
        """The main logic for cloning the project."""
        source_project_id = form_data['source_project_id']
        new_project_full_name = form_data['new_project_name']
        new_start_date = datetime.datetime.strptime(form_data['new_start_date'], '%Y-%m-%d')
        self.logger.info(f"Starting clone from source project ID: {source_project_id}")
        
        source_project = self.session.get('Project', source_project_id)
        
        new_end_date = None
        if source_project['start_date'] and source_project['end_date']:
            duration = source_project['end_date'] - source_project['start_date']
            new_end_date = new_start_date + duration
            self.logger.info(f"Calculated new end date '{new_end_date.date()}' based on source project duration.")
        else:
            self.logger.warning("Source project missing start/end dates. New end date will not be set.")
        
        if self.session.query(f'Project where full_name is "{new_project_full_name}"').first():
            raise ValueError(f"A project named '{new_project_full_name}' already exists.")

        job['data'] = json.dumps({'description': f"Creating project '{new_project_full_name}'..."})
        self.session.commit()

        new_project_short_name = new_project_full_name.lower().replace(' ', '_')
        self.logger.info(f"Creating new project entity: '{new_project_full_name}' (Short name: {new_project_short_name})")

        new_project = self.session.create('Project', {
            'name': new_project_short_name,
            'full_name': new_project_full_name,
            'project_schema': source_project['project_schema'],
            'start_date': new_start_date,
            'end_date': new_end_date
        })

        for key, value in source_project['custom_attributes'].items():
            new_project['custom_attributes'][key] = value
        self.logger.info(f"Copied custom attributes from source project.")

        self.session.commit()
        self.logger.info(f"New project created with ID: {new_project['id']}. Starting recursive copy.")

        self._clone_recursive(source_project, new_project)

    def _clone_recursive(self, source_parent, target_parent):
        """Recursively clones all children from a source parent to a target parent."""
        source_children = sorted(source_parent['children'], key=lambda child: child.get('position', 0))
        self.logger.info(f"Found {len(source_children)} children to copy under '{source_parent['name']}'.")
        for source_child in source_children:
            self.logger.info(f"Copying '{source_child['name']}' ({source_child.entity_type}) to '{target_parent['name']}'.")
            
            new_child_data = {'name': source_child['name'], 'parent': target_parent}

            if source_child.entity_type == 'Task':
                # --- MODIFICATIONS AS PER YOUR REQUEST ---

                # 1. Set the task type from the source.
                new_child_data['type'] = source_child.get('type')
                
                # 2. DO NOT copy the status. By leaving this out, ftrack will
                #    automatically assign the default "Not Started" status from the schema.
                
                # 3. DO NOT copy assignees. The 'assignments' attribute is not
                #    being copied, so new tasks will be unassigned.
                
                # --- END OF MODIFICATIONS ---

            new_child = self.session.create(source_child.entity_type, new_child_data)
            
            for key, value in source_child['custom_attributes'].items():
                new_child['custom_attributes'][key] = value

            self.session.commit()
            
            if source_child.entity_type not in ['Task', 'Milestone']:
                self._clone_recursive(source_child, new_child)

def register(session):
    """Register the project copy action."""
    logger.info("Registering Project Copy Action...")
    action = CreateProjectFromCopyAction(session)
    action.register() # The class's own register method
    logger.info("Project Copy Action registered.")