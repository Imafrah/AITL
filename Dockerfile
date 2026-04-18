# Use the official Python base image
FROM python:3.11-slim

# Set environment variables for production stability
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Render will automatically pass the PORT environment variable; this is a fallback.
ENV PORT=10000

# Set the working directory
WORKDIR /app

# Upgrade pip securely, then install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir -r requirements.txt

# Create a non-root user and group
RUN addgroup --system appgroup && adduser --system --group appuser

# Copy the application code
COPY . .

# Change ownership of the application code to the non-root user
RUN chown -R appuser:appgroup /app

# Switch to the non-root user for enhanced security
USER appuser

# Run the FastAPI server. 
# We use 'sh -c' to ensure the $PORT variable is correctly evaluated at runtime.
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT}"