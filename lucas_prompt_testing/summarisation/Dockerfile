FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install git and SSH for private repositories
RUN apt-get update && apt-get install -y git

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry for private repositories
ARG PYPI_USERNAME
ARG PYPI_PASSWORD
ARG PYPI_HOST

RUN echo "machine ${PYPI_HOST} login ${PYPI_USERNAME} password ${PYPI_PASSWORD}" > ~/.netrc && \
    chmod 600 ~/.netrc && \
    poetry config virtualenvs.create false && \
    poetry install --only=main --no-cache && \
    rm ~/.netrc


# Copy application code
COPY src/ ./src/

# Set working directory to ingestion
WORKDIR /app

# Define the command to run your script
CMD ["python", "-m", "src.summarisation.run_summarisation"]