import json

class Pretty:
    def __init__(self, json):
        self.json = json

    def print(self):
        return json.dumps(self.json, indent=4, sort_keys=True)