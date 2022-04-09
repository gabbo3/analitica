class Connector:
    def __init__(self,name) -> None:
        self.name = name

    def execute(self):
        print('Ejecutando: ' + self.name)