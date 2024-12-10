FROM python:3.10-slim AS python-base

LABEL maintainer="rodrigo.fuentes@globalfishingwatch.org"

# Fix vulns
RUN apt-get update -y && apt-get upgrade -y

# Upgrade pip
RUN pip install --upgrade pip

# https://python-poetry.org/docs#ci-recommendations
ENV POETRY_VERSION=1.8.3
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv

# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

# Create stage for Poetry installation
FROM python-base AS poetry-base

# Creating a virtual environment just for poetry and install it with pip
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install -U pip setuptools \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

# Create a new stage from the base python image
FROM poetry-base AS monorepo-base

# Copy Poetry to app image
COPY --from=poetry-base ${POETRY_VENV} ${POETRY_VENV}

# Add Poetry to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Set curdir to the app dir
WORKDIR /app

# Copy local libs dependencies and app inside the project folder
COPY ./libs ./libs
COPY ./pipe-vms-ingestion ./pipe-vms-ingestion

# Prepare the dev environment
FROM monorepo-base AS dev
WORKDIR /app/pipe-vms-ingestion
RUN poetry install
ENTRYPOINT ["poetry", "run", "python", "-u", "main.py"]

# Create the package wheel and source using poetry for distribution
FROM monorepo-base AS builder

# IMPORTANT: Move the contents of the package in the /app root, important 
# for building the package with the libs and update libs references with 
# path inside project folder needed by dataflow to cuild properly the 
# workflow.tar.gz package when deploying it on the workers
RUN mv ./pipe-vms-ingestion/* ./ && mv pyproject-build.toml pyproject.toml

# Build the app
RUN poetry build

# Create a new stage from the base python image
FROM python-base AS final

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.10_sdk:2.56.0 /opt/apache/beam /opt/apache/beam

# Copy app
COPY --from=builder /app /app

WORKDIR /app

# Install the app & deps in the python runtime
RUN pip install dist/*.whl

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
