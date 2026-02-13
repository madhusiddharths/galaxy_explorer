# Stage 1: Build the React Frontend
FROM node:20-alpine as build
WORKDIR /app/web
COPY web/package*.json ./
RUN npm install
COPY web/ ./
RUN npm run build

# Stage 2: Setup the Python Backend
FROM python:3.11-slim

# Install system dependencies if needed (e.g. for some python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy API constraints
COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

# Copy API code
COPY api ./api

# Copy built frontend assets from Stage 1 to api/static
COPY --from=build /app/web/dist ./api/static

# Expose port (Render sets PORT env var, but we default to 8000)
ENV PORT=8000
EXPOSE 8000

# Run the application
# We use search path "api.main:app" assuming we are in /app
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT}"]
