from facebookSource.Insight import Insight
import pandas as pd
from mappings.facebookSource.ComplexInsights import FbComplexInsights

class ComplexInsight(Insight):
	def asSQLDF(self):
		array = []
		for i in self.data:
			try:
				i['values'] = i.pop('value')
			except:
				continue
			for j in i['values']:
				fbList = dict(i)
				fbList['dimension'] = j
				fbList['value'] = i['values'][j]
				array.append(fbList)
		return pd.DataFrame(FbComplexInsights.clean(array))