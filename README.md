<h1 align="center" style="border-bottom: none;"> pipe-vms </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-vms">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-vms/branch/develop/graph/badge.svg?token=U9CFTTHZHA">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-vms">
  </a>
</p>

This repository contains the VMS pipeline, a dataflow pipeline that computes normalized positions for all countries and stores them in partitioned tables to use with v3.

---

---

## Project structure

The project contains the next structure

```
├── README.md
├── create-release.sh
├── docker-compose.yaml
├── nx.json
├── package-lock.json
├── package.json
└── packages
    ├── libs
    ├── pipe-vms-ingestion
    └── ...
```

At the root level, the repository contains common config files or dependencies like NX

- README.MD: Project basic documentation
- create-release.sh: Script to generate a tag/release for each project
- docker-compose.yaml: Docker configuration to run the projects locally.
- nx.json: Configuration related to NX library
- package.json and package-lock.json: NodeJS dependencies
- packages: This folder contains all the code related to the pipe-vms projects

Looking the package folder in detail, the repository contains 3 different folders

- libs: Common/shared code between all the projects

---

---

## Requirements

- [Docker](https://www.docker.com/get-started/)
- [NodeJS](https://github.com/nvm-sh/nvm)

Optional, if you do not use Docker, you need to install these dependencies

- [Python 3.9](https://www.python.org/downloads/release/python-390/)
- [Poetry](https://python-poetry.org/docs/)

---

## Usage

### Execute one project

```
docker compose up [service-name] [--build]
```

Example:

```
docker compose up pipe-vms-ingestion --build
```

Note: you can omit --build flag if you do not need to rebuild the docker image

---

### Create a release and push it to GitHub

This process will launch a Cloud Build trigger and generate the Docker image.

If you do not do this before, you need to install the NX dependencies

```
npm install
```

Once the NX dependencies were installed, then you can execute the `create-release.sh` script

```
./create-release [project-name] [tag-version]
```

For example:

```
./create-release pipe-vms-ingestion 1.0.0
```

This script will open an editor, and you can add release notes to there. Also, you can add release
notes in the Github page that will be launched after close this editor.

You must remove the `v` prefix that NX adds by default (it is an issue that their team needs to fix) and
create a new tag + create new release.

One trigger will be launching via CloudBuild and generate a docker image like this:

```
gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-vms:pipe-vms-ingestion-1.0.0
```

---

### Add a new project

```
yarn nx generate @nxlv/python:poetry-project [project_name] \
--projectType application \
--description='Project description' \
--packageName=[project-name] \
--moduleName=[project_name]
```

Example:

```
yarn nx generate @nxlv/python:poetry-project vms_ingestion \
--projectType application \
--description='VMS Ingestion processes' \
--packageName=pipe-vms-ingestion \
--moduleName=vms_ingestion
```

---

### Add a new library

```
yarn nx generate @nxlv/python:poetry-project [library_name] \
--projectType library \
--description='Library description' \
--packageName=[library-name] \
--moduleName=[library_name] \
--directory=libs
```

Example:

```
yarn nx generate @nxlv/python:poetry-project gfw-logger \
--projectType library \
--description='Library to reuse the GFW Logger' \
--packageName=gfw-logger \
--moduleName=gfw_logger \
--directory=libs
```

---

### Add a local library with a project

In order to use a shared library in the projects, you need to link the library with
the project.

```
yarn nx run [project-name]:add --name [lib-name] --local

```

Example:

```
yarn nx run publication-vms-ingestion:add --name libs-logger --local

```

Note: NX adds a prefix for each project based on the folder in which they are located. For instance,
publication for publication projects, core for core projects or libs for libs. This is something that you
need to take into account to link libs with projects.
