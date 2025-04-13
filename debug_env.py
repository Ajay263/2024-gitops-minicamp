import os
print(f"DBT_PROFILES_DIR: {os.environ.get('DBT_PROFILES_DIR', 'Not set')}")
print(f"PWD: {os.environ.get('PWD', 'Not set')}")