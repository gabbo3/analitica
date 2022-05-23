from mappings.instagram.Stats import StatsMapping


class Statistic:
	def __init__(self, data) -> None:
		self.data = data

	def asRawDict(self):
		return StatsMapping.raw(self.data)

	def asSQLDict(self):
		return StatsMapping.sql(self.data)