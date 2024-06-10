import json

import yaml


def parse_yaml_from_string(yaml_string):
    result = yaml.safe_load(yaml_string)
    if result == yaml_string:
        raise Exception("Invalid YAML")
    return result


def parse_json_from_string(json_string):
    return json.loads(json_string)
