import yaml
import os

class Config:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    @property
    def input_path(self):
        return self.config.get('input_path')

    @property
    def output_path(self):
        return self.config.get('output_path')

    @property
    def mode(self):
        return self.config.get('mode', 'local')

    @property
    def reference_point(self):
        return self.config.get('reference_point', {'lat': 0.0, 'lon': 0.0})
