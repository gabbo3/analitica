class Query:
    def __init__(self, data : dict) -> None:
        self.name = data['name']
        self.metrics = data['metrics']
        self.dimensions = data['dimensions']
        try:
            self.filters = data['filters']
        except:
            self.filters = None