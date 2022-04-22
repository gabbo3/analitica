from mappings.instagram.Post import PostMapping


class Post:
	def __init__(self, data):
		self.data = data

	def asRawDict(self) -> dict:
		return PostMapping.raw(self.data)

	def asSQLDict(self) -> dict:
		return PostMapping.sql(self.data)