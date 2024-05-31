import json

from jinja2 import Template


def get_schema(filepath):
    with open(filepath) as file:
        return json.load(file)


def get_sql_from_file(filepath, **query_args):

    with open(filepath) as f:
        sql_template = Template(f.read())
        output_template = sql_template.render(query_args)
        return output_template
