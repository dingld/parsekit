import re
import logging
import yaml
import jsonschema
from scrapy.utils.misc import arg_to_iter
from parsekit.processors import PROCS


logger = logging.getLogger(__name__)


def validate(instance):
    validator = get_validator()
    validator.check_schema(SCHEMA)
    validator.validate(instance)


def get_validator():
    """ Return jsonschema validator to validate SCHEMA """
    format_checker = jsonschema.FormatChecker()
    return jsonschema.Draft4Validator(SCHEMA, format_checker=format_checker)

def load_parser(path):
    if not hasattr(path, 'read'):
        path = open(path)
    return  yaml.load(path.read())

SCHEMA = {
    "type": "object",
    "properties": {
            "name": {"type": "string"},
            "maxpage": {"type": "integer"},
            "priority": {"type": "integer"},
            "rendering": {"type": "boolean"},
            "items": {
                "type": "array",
                "items": {"$ref": "#/defs/optionType"}
                },
            "links": {
                "type": "array",
                "items": {"$ref": "#/defs/optionType"}
                },
    },
    "required": ["name"],
    "defs": {
        "optionType": {
            "description": "items's schema to extract",
            "type": "object",
            "properties": {
                "nestedpath": {"type": "string"},
                "datatype": {'$ref': '#/defs/dataType'},
                'ref': {'type': 'boolean'}, # reference from items, recommended for links.
                'suffix': {'type': 'string'}, # suffix to add for links.
                'cond': {'type': 'object'}, # mongodb style condition
                "fields": {
                    'type': 'array',
                    'items': {'#ref': '#/defs/fieldType'}
                }
            },
            "oneOf":[
                 {"required": ["nestedpath", 'fields']},
                 {'required': ['ref']},
            ],
        },
        "fieldType": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "path": {"type": "string"},
                "re": {"type": "string"},
                "datatype": {'$ref': '#/defs/dataType'},
                "pipelines": {
                    "type": "array",
                    "items": {'type': 'string'},
                    },
                "required": {"type": "boolean"},
            },
            "required": ["key", "path"]
        },
        'dataType':{
            "type": "string", 
            'enum': ['innertext', 'url', 'href', 'src', 'html', 'int', 'float']
            },
    },
}


class DataTypes:
    ADDRESS = 'address'
    CURRENCY = 'currency'
    EMAIL = 'email'
    FLOAT = 'float'
    HREF = 'href'
    HTML = 'html'
    INNERTEXT = 'innertext'
    INT = 'int'
    SRC = 'src'
    URL = 'url'


CONDITION = {
    '$eq': lambda a,b: a == b,
    '$lt': lambda a,b: a < b,
    '$gt': lambda a,b: a > b,
    '$in': lambda a,b: a in b,
    '$re': lambda a,b: re.search(b, a)
}


Type2Pipe = {
    DataTypes.INNERTEXT: lambda pipes: ['strip', 'pjoin'] + pipes,
    DataTypes.SRC: lambda pipes: ['urljoin'] + pipes,
    DataTypes.HREF: lambda pipes: ['urljoin'] + pipes,
    DataTypes.URL: lambda pipes: ['urljoin'] + pipes,
    DataTypes.INT: lambda pipes: ['int'] + pipes,
    DataTypes.FLOAT: lambda pipes: ['float'] + pipes,    
}


def compile_fields(fields, funcs, option, links=False):
    option.setdefault('nestedpath', 'body')
    inputproc = option.get('inputproc', 'identity')
    outputproc = option.get('outputproc', 'takefirst')
    for field in fields:
        field.setdefault('inputproc', inputproc)
        field.setdefault('outputproc', outputproc)
        field.setdefault('path', '')
        if links and field.get('key') == 'url':
            field.setdefault('datatype', DataTypes.URL)
        field.setdefault('datatype', DataTypes.INNERTEXT)
        _compile_field(field, funcs)
    if option.get('conds'):
        for cond in option['conds']:
            _compile_field(cond, funcs, cond=True)

def filter_outout(item, conds, debug=False):
    required = _required_keys(item)
    conds = not conds or any(map(lambda cond:_satisfy_cond(item, cond), conds))
    return required and conds

def _compile_type(field):
    key = 'pipelines'
    field.setdefault(key, [])
    t = field.get('datatype')
    func = Type2Pipe.get(t)
    if callable(func):
        field[key] = func(field[key])

def _compile_field(field, funcs, cond=False):
    if not cond:
        _compile_type(field)
    inputproc = field.get('inputproc')
    outputproc = field.get('outputproc')
    if inputproc and isinstance(inputproc, str):
        field['inputproc'] = funcs[inputproc]
    if outputproc and isinstance(outputproc, str):
        field['outputproc'] = funcs[outputproc]
    pipelines = field.get('pipelines', [])
    if pipelines:
        field['pipelines'] = list(map(lambda x: funcs[x], pipelines))

def satisfy_condition(a, b, operator='$eq'):
    cond = CONDITION[operator]
    return a and all(cond(x, b) for x in arg_to_iter(a))

def _satisfy_cond(item, cond):
    each_condition = []
    for key, condition in cond.items(): # all keys satisfy
        singles = []
        if not key.startswith('$'):
            raise KeyError('Filter key $s illegal', key)
        key = key.strip('$')
        value = item.get(key)
        for operator, base in condition.items(): # all operators satisfy
            single = satisfy_condition(value, base, operator)
            singles.append(single)
        each_condition.append(all(singles))
    return all(each_condition)

def _required_keys(item):
    for key, field in item.fields.items():
        required = field.get('required')
        if required and not item.get(key):
            logger.warning('Required Field %s: %s', key, field)
            return False
    return True