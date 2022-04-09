from connectors.ConnectorFactory import ConnectorFactory

aConnectorFactory = ConnectorFactory()
aConnector = aConnectorFactory.createConnector('facebook')
aConnector.execute()