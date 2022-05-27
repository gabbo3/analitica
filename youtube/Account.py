class Account:
	def __init__(self,name):
		self.name = name
		self.reportList = loadReportList()

	def getReports(self):
		self.reports = []
		for r in self.reportList:
			self.reports.append(r)
		return self.reports