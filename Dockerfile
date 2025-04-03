# Use the official Python image as the base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock* README.md /app/
COPY src/movies_data_pipeline /app/movies_data_pipeline

# Install dependencies (excluding dev dependencies)
RUN poetry config virtualenvs.create false && poetry install --without dev

# Copy the rest of the application
COPY . /app

# Expose the port FastAPI will run on
EXPOSE 8000

# Command to run the app (will be overridden by docker-compose.yml)
CMD ["uvicorn", "movies_data_pipeline.api.main:app", "--host", "0.0.0.0", "--port", "8000"]