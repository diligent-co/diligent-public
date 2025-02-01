#!/usr/bin/env python3
import argparse
import subprocess
import os
from typing import Dict, Any

import yaml

this_dir = os.path.dirname(os.path.abspath(__file__))

def load_config(env: str) -> Dict[str, Any]:
    """Load configuration from the appropriate YAML file."""
    config_file = os.path.join(this_dir, '..', f'config.{env}.yaml')
    with open(config_file, encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config

def build_db_url(config: Dict[str, Any]) -> str:
    """Construct database URL from config."""
    pg = config['postgres']
    return (
        f"postgres://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
        f"?search_path=public&sslmode={pg['ssl']['mode']}"
    )

def run_migration(env: str, command: str) -> None:
    """Run Atlas migration command."""
    config = load_config(env)
    db_url = build_db_url(config)
    
    # Set up Atlas command
    atlas_cmd = [
        "atlas",
        command,  # 'migrate apply' or other commands
        "--url", db_url,
        "--dir", os.path.join(this_dir, '..', 'atlas', 'migrations')
    ]
    
    print(f"Running migrations for environment: {env}")
    try:
        subprocess.run(atlas_cmd, check=True)
        print("Migration completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"Migration failed: {e}")
        exit(1)

def main():
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument(
        '--env',
        choices=['local', 'prod'],
        required=True,
        help='Environment to run migrations against'
    )
    parser.add_argument(
        '--command',
        default='status',
        choices=['diff', 'hash', 'apply'],
        help='Atlas command to run'
    )
    
    args = parser.parse_args()
    run_migration(args.env, args.command)

if __name__ == "__main__":
    main()
