from configparser import ConfigParser


class _config():
    def __init__(self,environment):
        self.config = ConfigParser()
        self.config.read('./config/config.ini')
        self.config_values = dict(self.config.items(environment))

    def get_config(self) :
        return self.config_values