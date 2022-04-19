from googleAdServer.Report import Report
from mappings.googleAdServer.ReportVideo import DFPVideo


class ReportVideo(Report):
	def __init__(self):
		super().__init__()
		self.dimensions = ['DATE', 'AD_UNIT_NAME', 'POSITION_IN_POD', 'POSITION_OF_POD']

	def asSQLDF(self):
		return DFPVideo.sql(self.data)