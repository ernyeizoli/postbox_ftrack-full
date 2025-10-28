FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Mount all secrets in one command and build the .env file
RUN --mount=type=secret,id=FTRACK_SERVER \
    --mount=type=secret,id=FTRACK_API_USER \
    --mount=type=secret,id=FTRACK_API_KEY \
    --mount=type=secret,id=UNDARK_FTRACK_API_URL \
    --mount=type=secret,id=UNDARK_FTRACK_API_USER \
    --mount=type=secret,id=UNDARK_FTRACK_API_KEY \
    sh -c 'echo "FTRACK_SERVER=$(cat /run/secrets/FTRACK_SERVER)" > .env && \
           echo "FTRACK_API_USER=$(cat /run/secrets/FTRACK_API_USER)" >> .env && \
           echo "FTRACK_API_KEY=$(cat /run/secrets/FTRACK_API_KEY)" >> .env && \
           echo "UNDARK_FTRACK_API_URL=$(cat /run/secrets/UNDARK_FTRACK_API_URL)" >> .env && \
           echo "UNDARK_FTRACK_API_USER=$(cat /run/secrets/UNDARK_FTRACK_API_USER)" >> .env && \
           echo "UNDARK_FTRACK_API_KEY=$(cat /run/secrets/UNDARK_FTRACK_API_KEY)" >> .env'
 
# Ensure ftrack_api can write its schema cache. Set XDG cache to a writable location
ENV XDG_CACHE_HOME=/tmp
RUN mkdir -p /tmp/.cache && chown -R root:root /tmp/.cache

CMD ["python", "run_actions.py"]