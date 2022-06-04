from bisect import bisect_right
from collections import defaultdict
import op_costs 
from utils import *
from scenarios import *

MAX_ATTEMPTS = 3

class PrimaryCacheServer: 
	def __init__(self, scaling_strategy: str, maximum_capacity = 1000, num_machines=10, cache_size=100, replication_factor=2, hash_func=hash): 
		self.cache_size = cache_size
		self.maximum_capacity = maximum_capacity
		self.machine_memory_size = self.cache_size // num_machines
		self.scaling_strategy = scaling_strategy
		self.machines = []
		self.machine_hashes = []
		self.object_hashes = []
		self.object_hash_to_key = {}
		# Map from object name to set of machines responsible
		self.object_key_to_machine_hash = {}
		self.replication_factor = replication_factor
		self.hash_func = hash_func
		self.initMachines(scaling_strategy, num_machines)
		
	def initMachines(self, scaling_strategy: str, num_machines):
		if self.cache_size % num_machines != 0: 
			raise ValueError('cache_size %s does not evenly divide by requested number of machines %s', str(self.cache_size), str(num_machines))
		for _ in range(num_machines):
			self.add_node(scaling_strategy)

	def add_node(self, scaling_strategy):
		cost = 0
		cost += op_costs.add_node_cost()
		
		new_machine = WorkerCacheServer(scaling_strategy, self.machine_memory_size)
		new_machine_hash, hash_cost = self.getHash(new_machine.id)
		cost += hash_cost

		new_machine_index = bisect_right(self.machine_hashes, new_machine_hash)
		if new_machine_index > 0 and self.machine_hashes[new_machine_index - 1] == new_machine_hash:
			raise Exception("collision occurred")
		self.machine_hashes.insert(new_machine_index, new_machine_hash)
		self.machines.insert(new_machine_index, new_machine)
		print(f"Added node {new_machine_hash} to index {new_machine_index}")
		print(f"Updated machine hashes: {self.machine_hashes}")
		cost += self.migrate_data(new_machine_index)

		return cost

	def scaleCache(self, scaling_strategy: str): 
		cost = 0 
		if scaling_strategy == 'horizontal':
			# Add machine
			# TODO(santoshm): Create a function out of this. Try to make it more general than just to horizontal scaling? 
			if self.cache_size + self.machine_memory_size > self.maximum_capacity: 
				raise MemoryError('Cache cannot scale any further. Please allocate more capacity.')
			cost = self.add_node(scaling_strategy)
			self.cache_size += self.machine_memory_size
			return cost

	def migrate_data(self, dest_machine_index):
		cost = 0
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
			cost += self.move_objects(object_index_start, len(self.object_hashes), dest_machine, source_machine)
			cost += self.move_objects(0, object_index_end, dest_machine, source_machine)
		else:
			cost += self.move_objects(object_index_start, object_index_end, dest_machine, source_machine)
		return cost

	def move_objects(self, object_index_start, object_index_end, dest_machine, source_machine):
		cost = 0
		for object_index in range(object_index_start, object_index_end):
			object_hash_to_migrate = self.object_hashes[object_index]
			object_key_to_migrate = self.object_hash_to_key[object_hash_to_migrate]
			print(f"Moving object {object_hash_to_migrate} index {object_index} key {object_key_to_migrate}")
			print("Getting from source machine")
			object_to_migrate, op_cost = source_machine.get(object_key_to_migrate)
			cost += op_cost 

			# Redistribute hashes
			if self.scaling_strategy == 'replication_factor':
				print(f"Object key to machine hash: {self.object_key_to_machine_hash.keys()}")
				self.object_key_to_machine_hash[object_key_to_migrate].remove(self.getHash(source_machine.id))
				self.object_key_to_machine_hash[object_key_to_migrate].add(self.getHash(dest_machine.id))

			print("Inserting to destination machine")
			cost += dest_machine.insert(object_to_migrate)
			source_machine.pop(object_key_to_migrate)
			cost += op_costs.move_object_cost(object_to_migrate.size)
		return cost 

	def getHash(self, obj):
		return self.hash_func(obj) % self.maximum_capacity, op_costs.compute_hash_cost()

	# Consistent Hashing used to find the relevant machine for the requested object. This will get the first
	# valid machine.
	def getMachineIndex(self, obj_name: str):
		object_hash, hash_cost = self.getHash(obj_name)
		return (bisect_right(self.machine_hashes, object_hash)) % len(self.machines), hash_cost


	def insertObject(self, req: CacheInsertRequest, used_hashes=set()): 
		cost = 0
		new_object_hash = None
		counter = 0
		while new_object_hash not in used_hashes:
			new_object_hash, hash_cost = self.getHash(req.object.name + '_' + str(counter))
			cost += hash_cost
			used_hashes.add(new_object_hash)
			counter += 1

		machine_index = (bisect_right(self.machine_hashes, new_object_hash)) % len(self.machines)
		print(f"Attempting to insert new object {new_object_hash} to machine index {machine_index}")
		try: 
			cost += self.machines[machine_index].insert(req.object)
			self.object_hash_to_key[new_object_hash] = req.object.name
			object_index = bisect_right(self.object_hashes, new_object_hash)
			self.object_hashes.insert(object_index, new_object_hash)
			print(f"Added new object {new_object_hash} to index {object_index}")
			print(f"Updated object hashes: {self.object_hashes}")
		except MemoryError as e: 
			print(f"Scaling from {len(self.machines)} machines")
			cost += self.scaleCache(self.scaling_strategy)
			cost += self.Insert(req)

		return cost

	def Insert(self, req: CacheInsertRequest):
		cost = 0 

		if self.scaling_strategy == 'replication_factor': 
			if self.replication_factor == '': 
				return ValueError('No replication factor provided')
			# Generate replication factor (RF) number of hashes for the machines and objects
			object_key = req.object.name
			if object_key not in self.object_key_to_machine_hash:
				self.object_key_to_machine_hash[object_key] = set()
			used_hashes = self.object_key_to_machine_hash[object_key]
			for i in range(self.replication_factor):
				cost += self.insertObject(req, used_hashes)
		else: 
			cost += self.insertObject(req)

		return cost 

	def Get(self, req: CacheGetRequest):
		cost = 0
		
		counter = 0
		while counter < MAX_ATTEMPTS:
			try:
				machine_index, hash_cost = self.getMachineIndex(req.obj_name + '_' + str(counter))
				cost += hash_cost

				print(f"Getting {req.obj_name} from machine index {machine_index}")
				cachedObject, get_cost =  self.machines[machine_index].get(req.obj_name)
				cost += get_cost
			except Exception as e: 
				print(f"Getting {req.obj_name + '_' + str(counter)} from machine index {machine_index} failed")
				counter += 1
			break

		return cachedObject, cost

class WorkerCacheServer: 
	def __init__(self, scaling_strategy='horizontal', memory=100, load_threshold=5):
		self.id = uuid.uuid4()
		self.memory = memory 
		self.scaling_strategy = scaling_strategy
		self.objects = {}
		self.hitCounter = defaultdict(int)
		self.num_requests_processing = 0
		self.load_threshold = load_threshold

	def insert(self, obj: ObjectToCache): 
		cost = 0

		if self.scaling_strategy == 'horizontal': 
			if self.memory - obj.size < 0: 
				print("Memory error")
				raise MemoryError('Machine not large enough')

			else: 
				cost += op_costs.write_cost()
				self.memory -= obj.size
				obj_name = obj.name
				self.objects[obj_name] = obj

		return cost 

	def get(self, obj_name): 
		cost = 0

		self.hitCounter[obj_name] += 1
		cost += op_costs.read_cost()

		self.dump()
		return self.objects[obj_name], cost 

	def pop(self, obj_name):
		#TODO: Do we need to add a cost to remove an object, or is that negligible? 
		del self.objects[obj_name]
		self.dump()

	def dump(self):
		print(", ".join(self.objects.keys()))

if __name__ == '__main__': 
	pcs = PrimaryCacheServer('horizontal', 1000, 1, 1, 2)
	print(BasicWriteAndRead(0, pcs))
