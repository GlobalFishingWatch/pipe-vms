FROM python:3.9-slim as python-base

LABEL maintainer="rodrigo.fuentes@globalfishingwatch.org"

#ENV DEPS=" "
#RUN apt-get -qqy update && apt-get -qqy upgrade && apt-get install -y $DEPS

# https://python-poetry.org/docs#ci-recommendations
ENV POETRY_VERSION=1.8.3
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv

# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

# Create stage for Poetry installation
FROM python-base as poetry-base

# Creating a virtual environment just for poetry and install it with pip
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install -U pip setuptools \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

FROM apache/beam_python3.9_sdk:2.56.0 as beam_sdk

# Create a new stage from the base python image
FROM python-base as builder

# Copy files from official SDK image, including script/dependencies.
COPY --from=beam_sdk /opt/apache/beam /opt/apache/beam

# Copy Poetry to app image
COPY --from=poetry-base ${POETRY_VENV} ${POETRY_VENV}

# Add Poetry to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Set curdir to the app dir
WORKDIR /app/pipe-vms-ingestion/

# Copy Dependencies
COPY ./pipe-vms-ingestion/poetry.lock \
  ./pipe-vms-ingestion/pyproject.toml \
  ./pipe-vms-ingestion/README.md \
  ./

# Copy local libs dependencies
COPY ./libs ../libs

# [OPTIONAL] Validate the project is properly configured
RUN poetry check

# Install Dependencies and then the main app as advised 
# https://python-poetry.org/docs/faq#poetry-busts-my-docker-cache-because-it-requires-me-to-copy-my-source-files-in-before-installing-3rd-party-dependencies
RUN poetry install --only main --no-root --no-directory

# Copy Application
COPY ./pipe-vms-ingestion ./

# Install the app source code
RUN poetry install --only main

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
