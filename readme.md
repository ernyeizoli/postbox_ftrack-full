# Postbox Ftrack Automations – User Guide

The automation service runs continuously in the background. You do not need to start or configure anything; just use the features below inside Ftrack or Prism as usual.

## What You Can Expect

- **Project Copy Action**  
  In the Ftrack Actions menu you will see “Create Project from Copy.” It clones an existing project’s structure. Provide the new project name and start date when prompted; the action reports progress through the standard Ftrack Jobs panel.

- **Shot Task Template**  
  Whenever a Shot is added to a project, the automation drops in the default task trio (Animation, Lighting, Compositing). If any of those tasks already exist, they are left untouched.

- **UNDARK ↔ PBV Sync**  
  Tasks, notes, and asset versions published through Prism are mirrored between the UNDARK and PBV Ftrack servers. Key metadata such as `product` and `productpath` is copied so both sides see identical context. If the matching task does not exist on the other server, it is created automatically.

## How the Sync Works

- **Triggers**  
  The service subscribes to `ftrack.update` (tasks, versions) and `ftrack.note` events on both UNDARK and PBV. Whenever a new entity is added, the corresponding handler runs automatically.

- **Tasks**  
  New **asset-request** tasks created on PBV are replicated to the matching project on UNDARK. The sync checks for an existing task with the same name first to stay idempotent.

- **Notes**  
  When a note is posted on one server, the note handler finds the same project and task on the opposite side, merges the subject into the body (API requirement), and creates the note there once. The note ID is stored temporarily so the mirrored event is ignored and loops do not occur.

- **Asset Versions**  
  Publishing a version copies over the asset, project, task association, version number/name, comment, and Prism metadata. If the destination is missing the asset or task, they are generated on the fly (using the same task type when available) before the version is created.

- **Task Template Automation**  
  Shot creation in any project silently triggers the task template script, which attempts to fetch the new Shot up to five times (with one-second spacing) before adding the default Animation, Lighting, and Compositing tasks in a single batch commit.

- **Project Copy Action**  
  The menu action gathers form input, schedules a background job, clones the source project’s schema, structure, and custom attributes, and intentionally leaves statuses/assignments blank so the new project starts clean.

## Troubleshooting & Support

- If something fails to appear in Ftrack (missing tasks, notes, or versions), wait a minute and refresh; the sync may still be processing.
- Persistent issues or error pop-ups should be reported to the pipeline team—include project name, entity ID (if available), and a short description of what you expected to see.
- Users do not need to manage servers, credentials, or any terminal commands. Simply continue working in Ftrack and Prism; the backend scripts handle the rest.

Questions or requests for adjustments can be sent to **pipeline@postboxvisual.com**.