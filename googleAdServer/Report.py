from datetime import datetime, timedelta
import logging
import os
from googleads import ad_manager, errors
import tempfile
import pandas as pd


class Report:
	def __init__(self):
		self.endDate = datetime.now().date()
		self.startDate = self.endDate - timedelta(days=15)
		self.client = ad_manager.AdManagerClient.LoadFromStorage(str(os.getcwd()) + '/googleAdServer/googleads.yaml')
		
	def run(self):
		# Initialize a DataDownloader.
		report_downloader = self.client.GetDataDownloader(version='v202202')
		# Create report job.
		report_job = {
			'reportQuery': {
				'dimensions': self.dimensions,
				'adUnitView': 'FLAT',
				'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
							'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
				'dateRangeType': 'CUSTOM_DATE',
				'startDate': self.startDate,
				'endDate': self.endDate
			}
		}

		try:
			# Run the report and wait for it to finish.
			report_job_id = report_downloader.WaitForReport(report_job)
		except errors.AdManagerReportError as e:
			logging.error('Failed to generate report. Error was: %s' % e)

		report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
		export_format = 'CSV_DUMP'
		report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)
		report_file.close()
		df = pd.read_csv(report_file.name, compression='gzip')
		os.remove(report_file.name)
		self.data = df

	def asRawDF(self):
		return self.data