# Parseo los argumentos
import argparse
from datetime import datetime
import logging
import os
import traceback

from utils.sendMail import sendMail

parser = argparse.ArgumentParser(description="Analitica collect")
parser.add_argument("-s", action="store", dest="proceso", help="Store a simple value")
results = parser.parse_args()


# Logging Config
d = datetime.now()
logpath = os.getcwd() + "/logs/" + d.strftime('%Y%m%d')
os.makedirs(logpath, exist_ok=True)
logfilename = logpath + "/" + d.strftime('%Y%m%d%H%M%S') + "_" + results.proceso + ".log"
logname = d.strftime('%Y%m%d%H%M%S') + "_" + results.proceso + ".log"
print('Logging in: ' + logfilename)
logging.basicConfig(level=logging.INFO, filename=logfilename, format='%(levelname)s:%(asctime)s:%(funcName)s:%(lineno)d:%(message)s',datefmt='%Y-%m-%d %H:%M:%S')

from connectors.ConnectorFactory import ConnectorFactory

aConnectorFactory = ConnectorFactory()
aConnector = aConnectorFactory.createConnector(results.proceso)

try:
	aConnector.execute()

except Exception as e:
	logging.error(e, exc_info=True)
	traceback.print_exc()
	with open(logfilename, 'r') as file:
		log = file.read()
		sendMail(logname,log)

logging.info('FIN')
exit()