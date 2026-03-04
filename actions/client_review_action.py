import datetime
import ftrack_api
import logging


class AddToClientReviewAction(object):
    '''Action to add selected versions to today's client review session.'''

    label = "Add to Client Review"
    identifier = 'add.to.client.review'
    FOLDER_NAME = 'Dailies'

    def __init__(self, session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def register(self):
        self.session.event_hub.subscribe('topic=ftrack.action.discover', self.discover)
        self.session.event_hub.subscribe(
            f'topic=ftrack.action.launch and data.actionIdentifier={self.identifier}',
            self.launch
        )

    def discover(self, event):
        selection = event['data'].get('selection', [])
        if selection and selection[0]['entityType'] == 'assetversion':
            return {
                'items': [{
                    'label': self.label,
                    'actionIdentifier': self.identifier,
                    'icon': 'https://api.iconify.design/feather/eye.svg?color=%23ffffff'
                }]
            }

    def launch(self, event):
        selection = event['data'].get('selection', [])
        version_ids = [s['entityId'] for s in selection]

        versions = self.session.query(
            f'select id, project_id from AssetVersion where id in ({",".join(version_ids)})'
        ).all()
        if not versions:
            return {'success': False, 'message': 'No versions found.'}

        project_id = versions[0]['project_id']
        session_name = datetime.datetime.now().strftime('%Y_%m_%d')

        # 1. Find or create the "Dailies" ReviewSessionFolder
        dailies_folder = self.session.query(
            f'ReviewSessionFolder where name is "{self.FOLDER_NAME}" and project_id is "{project_id}"'
        ).first()

        if not dailies_folder:
            dailies_folder = self.session.create('ReviewSessionFolder', {
                'name': self.FOLDER_NAME,
                'project_id': project_id,
            })
            self.session.commit()
            self.logger.info('Created ReviewSessionFolder "%s"', self.FOLDER_NAME)

        # 2. Find or create today's ReviewSession inside that folder
        review_session = self.session.query(
            f'ReviewSession where name is "{session_name}" and project_id is "{project_id}"'
        ).first()

        if not review_session:
            review_session = self.session.create('ReviewSession', {
                'name': session_name,
                'project_id': project_id,
                'review_session_folder_id': dailies_folder['id'],
            })
            self.session.commit()
            self.logger.info('Created ReviewSession "%s"', session_name)

        # 3. Add each version to the review session (skip if already present)
        existing_ids = {
            obj['version_id']
            for obj in self.session.query(
                f'ReviewSessionObject where review_session_id is "{review_session["id"]}"'
            ).all()
        }

        added = 0
        for v in versions:
            if v['id'] not in existing_ids:
                self.session.create('ReviewSessionObject', {
                    'review_session_id': review_session['id'],
                    'version_id': v['id'],
                })
                added += 1

        self.session.commit()
        self.logger.info('Added %d version(s) to ReviewSession "%s"', added, session_name)
        return {'success': True, 'message': f'Added {added} version(s) to {session_name}'}


def register(session, **kw):
    if not isinstance(session, ftrack_api.Session):
        return
    action = AddToClientReviewAction(session)
    action.register()
