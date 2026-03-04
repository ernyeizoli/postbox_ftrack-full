import datetime
import ftrack_api
import logging

class AddToTodayDailiesAction(object):
    '''Action to create a daily list and add selected versions.'''
    
    label = "Add to Internal Daily"
    identifier = 'add.to.internal.dailies'

    def __init__(self, session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def register(self):
        self.session.event_hub.subscribe('topic=ftrack.action.discover', self.discover)
        self.session.event_hub.subscribe(f'topic=ftrack.action.launch and data.actionIdentifier={self.identifier}', self.launch)

    def discover(self, event):
        selection = event['data'].get('selection', [])
        # Only show if an AssetVersion is selected
        if selection and selection[0]['entityType'] == 'assetversion':
            return {'items': [{'label': self.label, 'actionIdentifier': self.identifier, 'icon': 'https://api.iconify.design/feather/calendar.svg?color=%23ffffff'}]}

    def launch(self, event):
        selection = event['data'].get('selection', [])
        version_ids = [s['entityId'] for s in selection]
        
        # Fetch versions and their project context
        versions = self.session.query(f'select project_id from AssetVersion where id in ({",".join(version_ids)})').all()
        if not versions:
            return {'success': False, 'message': 'No versions found.'}

        project_id = versions[0]['project_id']
        
        # 1. Find the "Dailies" list category in the current project.
        # ListCategory has no project_id, so we find it via an existing list
        # in this project that belongs to a category named "Dailies".
        sample_list = self.session.query(
            f'AssetVersionList where category.name is "Dailies" and project_id is "{project_id}"'
        ).first()

        if sample_list:
            dailies_category = sample_list['category']
        else:
            # No lists yet — fall back to a name-only lookup
            dailies_category = self.session.query(
                'ListCategory where name is "Dailies"'
            ).first()

        if not dailies_category:
            return {'success': False, 'message': 'Could not find a list category named "Dailies" in this project.'}

        # 2. Create/Find the daily list (daily_YYYYMMDD)
        list_name = f"daily_{datetime.datetime.now().strftime('%Y%m%d')}"

        # Check if the list already exists in that category
        daily_list = self.session.query(
            f'AssetVersionList where name is "{list_name}" and project_id is "{project_id}"'
        ).first()

        if not daily_list:
            daily_list = self.session.create('AssetVersionList', {
                'name': list_name,
                'project_id': project_id,
                'category_id': dailies_category['id']  # Put it inside the Dailies list category
            })
            self.session.commit()

        # 3. Add versions to the list
        for v in versions:
            if v not in daily_list['items']:
                daily_list['items'].append(v)

        self.session.commit()
        return {'success': True, 'message': f'Added to {list_name}'}

def register(session, **kw):
    if not isinstance(session, ftrack_api.Session):
        return
    action = AddToTodayDailiesAction(session)
    action.register()