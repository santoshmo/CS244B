import uuid
from datetime import datetime

class ObjectToCache:
	def __init__(self, name, size):
		self.size = size
		self.name = name
		self.id = uuid.uuid4()

class CacheInsertRequest: 
	def __init__(self, obj: ObjectToCache, background: bool=False): 
		self.object = obj
		self.background = background
		self.timestamp = datetime.now()

class CacheGetRequest:
	def __init__(self, obj_name): 
		self.obj_name = obj_name