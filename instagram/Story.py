from mappings.instagram.Story import StoryMapping


class Story:
	def __init__(self,data):
		self.data = data

	def asRawDict(self) -> dict:
		return StoryMapping.raw(self.data)

	def asSQLDict(self) -> dict:
		return StoryMapping.sql(self.data)