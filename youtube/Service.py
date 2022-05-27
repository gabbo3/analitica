from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from googleapiclient.discovery import build
import pandas as pd

class Service:
	def __init__(self,name, api_service_name, api_version, account_name, client_secrets_file, credentials_file,scopes):
		self.name = name
		self.api_service_name = api_service_name
		self.api_version = api_version
		self.account_name = account_name
		self.client_secrets_file = client_secrets_file
		self.credentials_file = credentials_file
		self.scopes = scopes
		self.build_resource()

	def build_resource(self):
		# Crear el flow
		flow = flow_from_clientsecrets(self.client_secrets_file, self.scopes)
		# Refrescar las credenciales
		storage = Storage(self.credentials_file)
		credentials = storage.get()
		if credentials is None or credentials.invalid:
			class flags:
				auth_host_name = "localhost"
				auth_host_port = [8080]
				noauth_local_webserver = False
				logging_level = "INFO"

			credentials = run_flow(flow, storage, flags)
		# Crear el servicio
		self.resource = build(self.api_service_name, self.api_version, credentials = credentials)