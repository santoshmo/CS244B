from bisect import bisect_right
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
		self.cache_size = cache_size
		self.maximum_capacity = maximum_capacity
		self.machine_memory_size = self.cache_size // num_machines
		self.scaling_strategy = scaling_strategy
		self.machines = []
		self.machine_hashes = []
		self.object_hashes = []
		self.object_hash_to_key = {}
		self.initMachines(scaling_strategy, num_machines)
		# TODO(santoshm): Add a map from object name to machine to make it easier to manage rehashing? 

	def initMachines(self, scaling_strategy: str, num_machines):
		if self.cache_size % num_machines != 0: 
			raise ValueError('cache_size %s does not evenly divide by requested number of machines %s', str(self.cache_size), str(num_machines))
		for _ in range(num_machines):
			self.add_node(scaling_strategy)

	def add_node(self, scaling_strategy):
		new_machine = WorkerCacheServer(scaling_strategy, self.machine_memory_size)
		new_machine_hash = self.getHash(new_machine.id)
		new_machine_index = bisect_right(self.machine_hashes, new_machine_hash)
		if new_machine_index > 0 and self.machine_hashes[new_machine_index - 1] == new_machine_hash:
			raise Exception("collision occurred")
		self.machine_hashes.insert(new_machine_index, new_machine_hash)
		self.machines.insert(new_machine_index, new_machine)
		print(f"Added node {new_machine_hash} to index {new_machine_index}")
		print(f"Updated machine hashes: {self.machine_hashes}")
		self.migrate_data(new_machine_index)

	def scaleCache(self, scaling_strategy: str): 
		if scaling_strategy == 'horizontal':
			# Add machine
			# TODO(santoshm): Create a function out of this. Try to make it more general than just to horizontal scaling? 
			if pcs.cache_size + pcs.machine_memory_size > pcs.maximum_capacity: 
				raise MemoryError('Cache cannot scale any further. Please allocate more capacity.')
			self.add_node(scaling_strategy)
			self.cache_size += self.machine_memory_size

	def migrate_data(self, dest_machine_index):
		left_machine_index = (dest_machine_index - 1) % len(self.machines)
		source_machine_index = (dest_machine_index + 1) % len(self.machines)
		left_machine_hash = self.machine_hashes[left_machine_index]
		dest_machine_hash = self.machine_hashes[dest_machine_index]
		object_index_start = bisect_right(self.object_hashes, left_machine_hash)
		object_index_end = bisect_right(self.object_hashes, dest_machine_hash)
		source_machine = self.machines[source_machine_index]
		dest_machine = self.machines[dest_machine_index]
		print(f"Migrating data between {left_machine_hash} and {dest_machine_hash} from source machine index {source_machine_index} to dest machine index {dest_machine_index}")
		if(left_machine_hash > dest_machine_hash):
			self.move_objects(object_index_start, len(self.object_hashes), dest_machine, source_machine)
			self.move_objects(0, object_index_end, dest_machine, source_machine)
		else:
			self.move_objects(object_index_start, object_index_end, dest_machine, source_machine)

	def move_objects(self, object_index_start, object_index_end, dest_machine, source_machine):
		for object_index in range(object_index_start, object_index_end):
			object_hash_to_migrate = self.object_hashes[object_index]
			object_key_to_migrate = self.object_hash_to_key[object_hash_to_migrate]
			print(f"Moving object {object_hash_to_migrate} index {object_index} key {object_key_to_migrate}")
			print("Source machine:")
			object_to_migrate = source_machine.get(object_key_to_migrate)
			dest_machine.insert(object_to_migrate)
			source_machine.pop(object_key_to_migrate)

	def getHash(self, input):
		return hash(input) % self.maximum_capacity

	# Consistent Hashing used to find the relevant machine for the requested object
	# TODO(santoshm): Implement load-aware consistent hashing. 
	# TODO(santoshm): Utilize some notion of the hotness of an object
	def getMachineIndex(self, obj_name: str):
		object_hash = self.getHash(obj_name)
		return (bisect_right(self.machine_hashes, object_hash)) % len(self.machines)

	def Insert(self, req: CacheInsertRequest):
		new_object_hash = self.getHash(req.object.name)
		machine_index = (bisect_right(self.machine_hashes, new_object_hash)) % len(self.machines)
		print(f"Attempting to insert new object {new_object_hash} to machine index {machine_index}")
		try: 
			self.machines[machine_index].insert(req.object)
			self.object_hash_to_key[new_object_hash] = req.object.name
			object_index = bisect_right(self.object_hashes, new_object_hash)
			self.object_hashes.insert(object_index, new_object_hash)
			print(f"Added new object {new_object_hash} to index {object_index}")
			print(f"Updated object hashes: {self.object_hashes}")
		except MemoryError as e: 
			print(f"Scaling from {len(self.machines)} machines")
			self.scaleCache(self.scaling_strategy)
			self.Insert(req)

	def Get(self, req: CacheGetRequest): 
		machine_index = self.getMachineIndex(req.obj_name)
		print(f"Getting {req.obj_name} from machine index {machine_index}")
		return self.machines[machine_index].get(req.obj_name)

class WorkerCacheServer: 
	def __init__(self, scaling_strategy='horizontal', memory=100):
		self.id = uuid.uuid4()
		self.memory = memory 
		self.scaling_strategy = scaling_strategy
		self.objects = {}
		self.hitCounter = defaultdict(int)

	def insert(self, obj: ObjectToCache): 
		if self.memory - obj.size < 0: 
			if self.scaling_strategy == 'horizontal': # TODO(santoshm): Make this an Enum
				print("Memory error")
				raise MemoryError('Machine not large enough')
		else: 
			self.memory -= obj.size
			obj_name = obj.name
			self.objects[obj_name] = obj

	def get(self, obj_name): 
		self.hitCounter[obj_name] += 1
		self.dump()
		return self.objects[obj_name]

	def pop(self, obj_name):
		del self.objects[obj_name]
		self.dump()

	def dump(self):
		print(", ".join(self.objects.keys()))

if __name__ == '__main__': 
	pcs = PrimaryCacheServer('horizontal', 1000, 1, 1)
	print("Adding first object")
	pcs.Insert(CacheInsertRequest(ObjectToCache('first object', 1)))
	print("Adding second object")
	pcs.Insert(CacheInsertRequest(ObjectToCache('second object', 1)))
	print("Getting first object")
	assert(pcs.Get(CacheGetRequest('first object')).name, 'first object')
	print("Getting second object")
	assert(pcs.Get(CacheGetRequest('second object')).name, 'second object')
