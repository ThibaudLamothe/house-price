class ImmoScrap:
    def __init__(self, config_path):
        self.config_path = self.parse_config(config_path)
        self.data_path = self.config_path['DATA_PATH']
        self.dash_token = self.config_path['DASH_TOKEN']
        self.slack_token = self.config_path['SLACK_TOKEN']

    def parse_config(self, path):
        return path

    def add_slack_token(self, token):
        self.slack_token = token

    def add_dash_token(self, token):
        self.dash_token = token

    def add_request(self, website, request_dict):
        return None

    def see_requests(self):
        return None

    def delete_request(self):
        return None

    def activate_alerting(self, hourly=False, daily=False, weekly=False):
        return None

    def run(self):
        return None

    def run_viz(self):
        return None


if __name__ == "__main__":
    immo = ImmoScrap('config.json')
