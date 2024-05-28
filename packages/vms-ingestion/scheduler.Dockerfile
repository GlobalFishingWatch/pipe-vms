FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest-python3.9

LABEL maintainer="rodrigo.fuentes@globalfishingwatch.org"

#ENV DEPS=" "
#RUN apt-get -qqy update && apt-get -qqy upgrade && apt-get install -y $DEPS

WORKDIR /app/builder

COPY ./libs ./packages/libs
COPY ./vms-ingestion ./packages/vms-ingestion

WORKDIR /app/builder/packages/vms-ingestion

RUN pip3 install poetry && poetry install

ENTRYPOINT ["poetry", "run", "python", "-u", "main.py"]









