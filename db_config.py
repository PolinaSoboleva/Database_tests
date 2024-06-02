import yaml

def load_db_config(env):
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config['databases'][env]