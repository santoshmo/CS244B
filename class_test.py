class TestCacheServer: 
	def __init__(self, hash_func=hash):
		self.hash_func = hash_func
	def getHash(self, obj):
		return self.hash_func(obj)
if __name__ == '__main__': 
	s = TestCacheServer()
	print(s.getHash('string'))