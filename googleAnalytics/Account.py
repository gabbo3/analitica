class Account:
    def __init__(self, data : dict) -> None:
        self.name = data['name']
        self.id = data['id']