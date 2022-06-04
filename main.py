from server import PrimaryCacheServer
from scenarios import * 

if __name__ == '__main__': 
	pcs = PrimaryCacheServer('horizontal', 1000, 1, 1, 2)
	print(BasicWriteAndRead(0, pcs))