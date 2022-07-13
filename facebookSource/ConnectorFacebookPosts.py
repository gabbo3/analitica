import logging
import pandas as pd
from facebookSource.Account import Account
from facebookSource.ConnectorFacebook import ConnectorFacebook
from facebookSource.Post import Post
from tqdm import tqdm

class ConnectorFacebookPosts(ConnectorFacebook):

	def __init__(self) -> None:
		super().__init__()
		self.loadIDS()
	
	def execute(self):
		upload_array = []
		for i in tqdm(self.ids):
			# Para cada id identificamos a que cuenta pertenece
			a = self.getAccount(i)
			p = Post(name= a.name,id=i,token=a.token)
			self.mongo.upsertDict(p.asRawDict(),'RAWDATA','FB_POSTS_HISTORICO_TEST')
			try:
				upload_array.append(p.asSQLDict())
			except Exception as e:
				logging.error(e, exc_info=True)
				logging.info(p.data)
		self.sql.upsert(pd.DataFrame(upload_array),'PY_FB_POSTS')

	def loadIDS(self):
		filepath = '../FB Posteos mensuales/202205.csv'
		csv = pd.read_csv(filepath)
		self.ids = ['_'.join(i) for i in zip(csv["Identificador de la página"].map(str),csv["Identificador de la publicación"].map(str))]

	def getAccount(self,id : str):
		account_id = id.split('_')[0]
		if account_id == '142725972411814':
			return self.accounts[0]
		if account_id == '143064249067032':
			return self.accounts[1]
		if account_id == '713293215476114':
			return self.accounts[2]
		if account_id == '152876814742559':
			return self.accounts[3]
		if account_id == '136249719746178':
			return self.accounts[4]
		if account_id == '1584691088491568':
			return self.accounts[5]
		if account_id == '991161774315431':
			return self.accounts[6]
		if account_id == '561386987398491':
			return self.accounts[7]
		if account_id == '1738550376405782':
			return self.accounts[8]
		if account_id == '1615597355436173':
			return self.accounts[9]
		if account_id == '1541289102847506':
			return self.accounts[10]
		if account_id == '236457393517825':
			return self.accounts[11]
		if account_id == '1573288136302308':
			return self.accounts[12]
		if account_id == '1090915624358855':
			return self.accounts[13]
		if account_id == '275052512523038':
			return self.accounts[14]
		if account_id == '550808478293870':
			return self.accounts[15]
		if account_id == '394828987663815':
			return self.accounts[16]
		if account_id == '1397179786993067':
			return self.accounts[17]
		if account_id == '1597908767128946':
			return self.accounts[18]
		if account_id == '270411186673566':
			return self.accounts[19]
		if account_id == '307159023083898':
			return self.accounts[20]
		if account_id == '234163610340060':
			return self.accounts[21]
		if account_id == '558401927533514':
			return self.accounts[22]

