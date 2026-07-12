import re

with open('docker-compose.fresh-test.yml', 'r') as f:
    text = f.read()

# Remove env_file from postgres
text = re.sub(r'  postgres:\n    image: postgres:15\n    container_name: linkedin-fresh-postgres\n    env_file:\n      - \.fresh-test/\.env', 
              '  postgres:\n    image: postgres:15\n    container_name: linkedin-fresh-postgres', text)

# Remove env_file from dashboard
text = re.sub(r'  dashboard:(\n.*?)*?env_file:\n      - \.fresh-test/\.env\n    environment:', 
              '  dashboard:\\1    environment:', text)

with open('docker-compose.fresh-test.yml', 'w') as f:
    f.write(text)
