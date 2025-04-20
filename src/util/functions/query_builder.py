import yaml
from jinja2 import Template

def load_variables(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

def render_query(template_path, variables):
    with open(template_path, 'r') as file:
        template = Template(file.read())
    return template.render(**variables)

def merge_with_defaults(global_vars, query_name):
    defaults = global_vars.get("defaults", {})
    if query_name not in global_vars:
        raise ValueError(f"Variables para '{query_name}' no encontradas en el YAML.")
    specifics = global_vars.get(query_name) or {}
    return {**defaults, **specifics}

