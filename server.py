import asyncio
from bisect import bisect_right
from datetime import datetime
import random
import re
import uuid
import pdb
from collections import defaultdict
import op_costs 
from scenarios import *

# REPLICATION_FACTOR = 3 # Not used.

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
		self.init_machines_num = num_machines

	async def initMachines(self, cost: int = 0, scaling_strategy: str=None, num_machines=None):
		if scaling_strategy is None:
			scaling_strategy = self.scaling_strategy
		if num_machines is None:
			num_machines = self.init_machines_num
		if self.cache_size % num_machines != 0: 
			raise ValueError('cache_size %s does not evenly divide by requested number of machines %s', str(self.cache_size), str(num_machines))
		await asyncio.gather(*[self.add_node(scaling_strategy) for _ in range(num_machines)])

	async def add_node(self, scaling_strategy):
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
		cost += await self.migrate_data(new_machine_index) # TODO: Decide what to do for replication
		return cost

	def remove_node(self, node_index=-1):
		if node_index == -1:
			node_index = random.randrange(0, len(self.machines))
		if node_index >= len(self.machines):
			raise Exception(f"Can't remove machine {node_index} from {len(self.machines)} machines.")
		self.machine_hashes.pop(node_index)
		self.machines.pop(node_index)
		print(f"Removed node {node_index}")
		print(f"Updated machine hashes: {self.machine_hashes}")

	async def scaleCache(self, scaling_strategy: str): 
		cost = 0 
		if scaling_strategy == 'horizontal':
			# Add machine
			# TODO(santoshm): Create a function out of this. Try to make it more general than just to horizontal scaling? 
			if self.cache_size + self.machine_memory_size > self.maximum_capacity: 
				raise MemoryError('Cache cannot scale any further. Please allocate more capacity.')
			cost = await self.add_node(scaling_strategy)
			self.cache_size += self.machine_memory_size
			return cost

	async def migrate_data(self, dest_machine_index):
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
			cost += await self.move_objects(object_index_start, len(self.object_hashes), dest_machine, source_machine)
			cost += await self.move_objects(0, object_index_end, dest_machine, source_machine)
		else:
			cost += await self.move_objects(object_index_start, object_index_end, dest_machine, source_machine)
		return cost

	async def move_objects(self, object_index_start, object_index_end, dest_machine, source_machine):
		cost = 0
		for object_index in range(object_index_start, object_index_end):
			object_hash_to_migrate = self.object_hashes[object_index]
			object_key_to_migrate = self.object_hash_to_key[object_hash_to_migrate]
			print(f"Moving object {object_hash_to_migrate} index {object_index} key {object_key_to_migrate}")
			print("Source machine:")
			object_to_migrate, op_cost = source_machine.get(object_key_to_migrate)
			cost += op_cost 

			cost += await dest_machine.insert(object_to_migrate)
			source_machine.pop(object_key_to_migrate)
			cost += op_costs.move_object_cost(object_to_migrate.size)
		return cost

	# Not used. Decide what to do for replication during node addition
	async def replicate_data(self, dest_machine_index):
		cost = 0
		source_machine_index = (dest_machine_index - 1) % len(self.machines)
		source_machine = self.machines[source_machine_index]
		dest_machine = self.machines[dest_machine_index]
		for object_index in range(0, len(self.object_hashes)):
			object_hash_to_migrate = self.object_hashes[object_index]
			object_key_to_migrate = self.object_hash_to_key[object_hash_to_migrate]
			print(f"Moving object {object_hash_to_migrate} index {object_index} key {object_key_to_migrate}")
			print("Source machine:")
			object_to_migrate, op_cost = source_machine.get(object_key_to_migrate)
			cost += op_cost 

			cost += await dest_machine.insert(object_to_migrate)
			cost += op_costs.move_object_cost(object_to_migrate.size)
		return cost
	
	# Not used. Decide what to do for replication during insertion
	async def replicate_datum(self, object_to_migrate, source_machine_index, replication_factor):
		cost = 0
		dest_machine_index = (source_machine_index + 1) % len(self.machines)
		dest_machine = self.machines[dest_machine_index]
		cost += await asyncio.gather(*[dest_machine.insert(object_to_migrate) for _ in range(0, replication_factor)
])
		cost += op_costs.move_object_cost(object_to_migrate.size) * replication_factor # TODO: Is this right?
		return cost

	def getHash(self, input):
		return hash(input) % self.maximum_capacity, op_costs.compute_hash_cost()

	# Consistent Hashing used to find the relevant machine for the requested object
	# TODO(santoshm): Implement load-aware consistent hashing. 
	# TODO(santoshm): Utilize some notion of the hotness of an object
	def getMachineIndex(self, obj_name: str):
		object_hash, hash_cost = self.getHash(obj_name)
		return (bisect_right(self.machine_hashes, object_hash)) % len(self.machines), hash_cost

	async def Insert(self, req: CacheInsertRequest):
		cost = 0 

		new_object_hash, hash_cost = self.getHash(req.object.name)
		cost += hash_cost

		machine_index = (bisect_right(self.machine_hashes, new_object_hash)) % len(self.machines)
		print(f"Attempting to insert new object {new_object_hash} to machine index {machine_index}")
		try: 
			cost += await self.machines[machine_index].insert(req.object)
			self.object_hash_to_key[new_object_hash] = req.object.name
			object_index = bisect_right(self.object_hashes, new_object_hash)
			self.object_hashes.insert(object_index, new_object_hash)
			print(f"Added new object {new_object_hash} to index {object_index}")
			print(f"Updated object hashes: {self.object_hashes}")
			# self.replicate_datum(req.object, machine_index, REPLICATION_FACTOR) # TODO: What do we do for replication?
		except MemoryError as e: 
			print(f"Scaling from {len(self.machines)} machines")
			cost += await self.scaleCache(self.scaling_strategy)
			cost += await self.Insert(req)

		return cost 

	def Get(self, req: CacheGetRequest):
		cost = 0
		
		machine_index, hash_cost = self.getMachineIndex(req.obj_name)
		cost += hash_cost

		print(f"Getting {req.obj_name} from machine index {machine_index}")
		cachedObject, get_cost =  self.machines[machine_index].get(req.obj_name)
		cost += get_cost

		return cachedObject, cost

class WorkerCacheServer: 
	def __init__(self, scaling_strategy='horizontal', memory=100):
		self.id = uuid.uuid4()
		self.memory = memory 
		self.scaling_strategy = scaling_strategy
		self.objects = {}
		self.hitCounter = defaultdict(int)

	async def insert(self, obj: ObjectToCache): 
		cost = 0
		if self.memory - obj.size < 0: 
			if self.scaling_strategy == 'horizontal': # TODO(santoshm): Make this an Enum
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

async def main():
	pcs = PrimaryCacheServer('horizontal', 1000, 1, 1)
	await pcs.initMachines()
	scenario_cost = await BasicWriteAndRead(0, pcs)
	print(scenario_cost)

if __name__ == '__main__':
	asyncio.run(main())
