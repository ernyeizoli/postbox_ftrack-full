"""
Copy Lock Module
Provides a file-based lock to indicate when a project copy is in progress.
Uses a file-based approach so it works across multiple processes.
Other actions can check this flag and skip processing during copy operations.
"""

import os
import tempfile

# Use a lock file in a temp directory that persists across process restarts
LOCK_FILE = os.path.join(tempfile.gettempdir(), "ftrack_copy_in_progress.lock")


def is_copy_in_progress():
    """Returns True if a project copy is currently in progress."""
    return os.path.exists(LOCK_FILE)


def set_copy_in_progress(value: bool):
    """Sets the copy-in-progress flag by creating/removing a lock file."""
    if value:
        # Create the lock file
        try:
            with open(LOCK_FILE, 'w') as f:
                f.write("copy in progress")
        except Exception:
            pass
    else:
        # Remove the lock file
        try:
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except Exception:
            pass
