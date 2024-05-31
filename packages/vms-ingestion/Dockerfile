FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest-python3.9

LABEL maintainer="rodrigo.fuentes@globalfishingwatch.org"

#ENV DEPS=" "
#RUN apt-get -qqy update && apt-get -qqy upgrade && apt-get install -y $DEPS

WORKDIR /app/builder

COPY ./libs ./packages/libs
COPY ./vms-ingestion ./packages/vms-ingestion

WORKDIR /app/builder/packages/vms-ingestion

RUN pip3 install poetry && poetry install

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]

# Setup the entrypoint for quickly executing the pipelines
# ENTRYPOINT ["poetry", "run", "python", "-u", "main.py"]









