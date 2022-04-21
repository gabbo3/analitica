from googleAdServer.Report import Report
from mappings.googleAdServer.ReportNoVideo import DFPNoVideo


class ReportNoVideo(Report):
	def __init__(self):
		super().__init__()
		self.dimensions = ['DATE', 'AD_UNIT_NAME']

	def asRawDF(self):
		return DFPNoVideo.raw(self.data)
		
	def asSQLDF(self):
		return DFPNoVideo.sql(self.data)