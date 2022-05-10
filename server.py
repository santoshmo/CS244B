from datetime import datetime
import uuid
import pdb
from collections import defaultdict

class ObjectToCache:
	def __init__(self, name, size):
		self.size = size
		self.name = name
		self.id = uuid.uuid4()

class CacheInsertRequest: 
	def __init__(self, obj: ObjectToCache): 
		self.object = obj
		self.timestamp = datetime.now()

class CacheGetRequest:
	def __init__(self, obj_name): 
		self.obj_name = obj_name

class PrimaryCacheServer: 
	def __init__(self, scaling_strategy: str, maximum_capacity = 1000, num_machines=5, cache_size=100): 
		self.num_machines = num_machines
		self.cache_size = cache_size
		self.maximum_capacity = maximum_capacity
		self.machine_memory_size = self.cache_size // self.num_machines
		self.machines = self.initMachines(scaling_strategy)
		self.scaling_strategy = scaling_strategy
		# TODO(santoshm): Add a map from object name to machine to make it easier to manage rehashing? 

	def initMachines(self, scaling_strategy: str):
		if self.cache_size % self.num_machines != 0: 
			raise ValueError('cache_size %s does not evenly divide by requested number of machines %s', str(self.cache_size), str(self.num_machines))
		return [WorkerCacheServer(scaling_strategy, self.cache_size // self.num_machines) for machine in range(self.num_machines)]

	def scaleCache(self, scaling_strategy: str): 
		if scaling_strategy == 'horizontal': 
			self.cache_size += self.machine_memory_size
			self.machines.append(WorkerCacheServer(scaling_strategy, self.cache_size // self.num_machines))

	# Consistent Hashing used to find the relevant machine for the requested object
	# TODO(santoshm): Implement load-aware consistent hashing. 
	# TODO(santoshm): Utilize some notion of the hotness of an object
	def getMachineId(self, obj_name: str):
		return hash(obj_name) % self.num_machines

	def Insert(self, req: CacheInsertRequest):
		machine_id = self.getMachineId(req.object.name)
		try: 
			self.machines[machine_id].insert(req.object)
		except MemoryError as e: 
			print("Scaling from %s machines", str(self.num_machines))
			self.scaling_strategy.scaleCache(self)
			Insert(self, req)


	def Get(self, req: CacheGetRequest): 
		machine_id = self.getMachineId(req.obj_name)
		return self.machines[machine_id].get(req.obj_name)

class WorkerCacheServer: 
	def __init__(self, scaling_strategy: str, memory=100): 
		self.memory = memory 
		self.scaling_strategy = ScalingStrategy(scaling_strategy)
		self.objects = {}
		self.hitCounter = defaultdict(int)

	def insert(self, obj: ObjectToCache): 
		if self.memory - obj.size < 0: 
			if self.scaling_strategy == 'horizontal': # TODO(santoshm): Make this an Enum
				raise MemoryError('Machine not large enough')
		else: 
			self.memory -= obj.size
			self.objects[obj.name] = obj

	def get(self, obj_name): 
		self.hitCounter[obj_name] += 1
		return self.objects[obj_name]

class ScalingStrategy: 
	def __init__(self, scaling_strategy='horizontal'): 
		self.scaling_strategy = scaling_strategy

	def scaleCache(self, pcs: PrimaryCacheServer): 
		if self.scaling_strategy == 'horizontal': 
			# Add machine
			# TODO(santoshm): Create a function out of this. Try to make it more general than just to horizontal scaling? 
			if pcs.cache_size + pcs.machine_memory_size > pcs.maximum_capacity: 
				raise MemoryError('Cache cannot scale any further. Please allocate more capacity.')
			pcs.num_machines += 1
			pcs.cache_size += pcs.machine_memory_size
			pcs.machines.append(WorkerCacheServer(scaling_strategy, pcs.machine_memory_size))

if __name__ == '__main__': 
	pcs = PrimaryCacheServer('horizontal', 1000, 1, 1)
	print("Adding first object")
	pcs.Insert(CacheInsertRequest(ObjectToCache('first object', 1)))
	print("Adding second object")
	pcs.Insert(CacheInsertRequest(ObjectToCache('second object', 1)))
	print("Getting first object")
	assert(pcs.Get(CacheGetRequest('first object')).name, 'first object')



