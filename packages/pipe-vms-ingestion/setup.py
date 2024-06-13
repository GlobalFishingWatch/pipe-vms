#!/usr/bin/env python
from setuptools import find_packages, setup

# packages = \
#     ['vms_ingestion',
#      'vms_ingestion.normalization',
#      'vms_ingestion.normalization.feeds',
#      'vms_ingestion.normalization.transforms']

# package_data = \
#     {'': ['*']}

# install_requires = \
#     ['apache-beam[gcp]==2.56.0',
#      'bigquery @ ../libs/bigquery',
#      'jinja2>=3.0.3,<4.0.0',
#      'logger @ ../libs/logger',
#      'utils @ ../libs/utils']

setup_kwargs = {
    'name': 'pipe-vms-ingestion',
    'version': '1.0.0',
    'description': 'VMS Ingestion',
    'long_description': '# pipe-vms-ingestion\n\nProject description here.\n',
    'author': 'None',
    'author_email': 'None',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'None',
    'packages': find_packages(exclude=['test*.*', 'tests']),
    # 'package_data': package_data,
    # 'install_requires': install_requires,
    'python_requires': '>=3.9,<3.11',
}


setup(**setup_kwargs)
