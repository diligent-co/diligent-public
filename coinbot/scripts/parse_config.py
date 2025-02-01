import os
import sys
import json
import yaml

this_dir = os.path.dirname(os.path.abspath(__file__))

def parse_config(env: str):
    with open(os.path.join(this_dir, '..', f'config.{env}.yaml'), encoding='utf-8') as f: 
        config = yaml.safe_load(f)
    return { 
        'user': config['postgres']['user'], 
        'password': config['postgres']['password'], 
        'host': config['postgres']['host'], 
        'port': str(config['postgres']['port']), 
        'database': config['postgres']['database'],
        'ssl_mode': config['postgres']['ssl']['mode']
    }

if __name__ == '__main__':
    env = sys.argv[1]
    if env not in ['local', 'prod']:
        raise ValueError(f'Invalid environment: {env}')
    print(json.dumps(parse_config(env)))
