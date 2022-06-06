import asyncio
from bisect import bisect_right
from collections import defaultdict
import hashlib
from statistics import mean
import sys
import op_costs 
from utils import *
from scenarios import *
import random
import uuid
import pdb

IS_DEBUG = False

NUM_RETRIES = 3

def debug(str):
	if IS_DEBUG:
		print(str)

class HitStatistics: 
	def __init__(self):
		self.num_total_instances = 0
		self.num_current_hits = 0

class PrimaryCacheServer: 
	def __init__(self, scaling_strategy: str, maximum_capacity = 1000, num_machines=10, cache_size=100, replication_factor=2, num_hashes_per_node=5, hash_func=hash, object_load_threshold=1, request_type='hedging'): 
		self.cache_size = cache_size
		self.maximum_capacity = maximum_capacity
		self.machine_memory_size = self.cache_size // num_machines
		self.scaling_strategy = scaling_strategy
		self.machines = []
		self.machine_hashes = []
		self.machine_id_to_indices = defaultdict(list)
		self.object_hashes = []
		self.object_hash_to_key = {}
		self.replication_factor = replication_factor
		self.hash_func = hash_func
		self.num_machine_hashes_per_node = num_hashes_per_node
		self.init_machines_num = num_machines
		self.object_load_threshold = object_load_threshold
		self.obj_hitstats = defaultdict(HitStatistics) # (num_total_instances, number_current_hits)
		self.super_hot_obj_to_machine_id = {}
		self.request_type = request_type
		self.db = {} # store of all objects ever written to add to the cache after cache miss.

	async def initMachines(self, scaling_strategy: str=None, num_machines=None):
		if scaling_strategy is None:
			scaling_strategy = self.scaling_strategy
		if num_machines is None:
			num_machines = self.init_machines_num
		if self.cache_size % num_machines != 0: 
			raise ValueError('cache_size %s does not evenly divide by requested number of machines %s', str(self.cache_size), str(num_machines))
		await asyncio.gather(*[self.add_node(scaling_strategy) for _ in range(num_machines)])
		debug("Done init machines")

	async def add_node(self, scaling_strategy):
		cost = 0
		cost += op_costs.add_node_cost()
		
		new_machine = WorkerCacheServer(scaling_strategy, self.machine_memory_size)

		counter = 0
		success = False
		while counter < self.num_machine_hashes_per_node:
			try:
				new_machine_hash, hash_cost = self.getHash(str(new_machine.id) + '_' + str(counter))
				cost += hash_cost

				new_machine_index = bisect_right(self.machine_hashes, new_machine_hash)
				if new_machine_index > 0 and self.machine_hashes[new_machine_index - 1] == new_machine_hash:
					raise Exception(f"collision occurred. Adding new machine hash {new_machine_hash} index {new_machine_index} to machine hashes {self.machine_hashes} failed")
				self.machine_hashes.insert(new_machine_index, new_machine_hash)
				self.machines.insert(new_machine_index, new_machine)
				self.machine_id_to_indices[new_machine.id].append((new_machine, new_machine_hash))
				debug(f"Added node {new_machine_hash} to index {new_machine_index}")
				debug(f"Updated machine hashes: {self.machine_hashes}")
				cost += await self.migrate_data(new_machine_index)
				success = True
			except Exception as e:
				debug(f"Exception: {e.__class__} {e}")
			counter += 1

		if not success:
			raise Exception("Adding a new node failed")
		debug(f"Done adding machine {new_machine.id}")
		return cost

	def remove_node(self, node_index=-1):
		if node_index == -1:
			node_index = random.randrange(0, len(self.machines))
		if node_index >= len(self.machines):
			raise Exception(f"Can't remove machine {node_index} from {len(self.machines)} machines.")
		machine_id = self.machines[node_index].id
		for machine, machine_hash in self.machine_id_to_indices[machine_id]:
			self.machine_hashes.remove(machine_hash)
			self.machines.remove(machine)
			debug(f"Removed node hash {machine_hash}")
		debug(f"Updated machine hashes: {self.machine_hashes}")

	async def scaleCache(self, scaling_strategy: str): 
		cost = 0 
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
		debug(f"Migrating data between {left_machine_hash} and {dest_machine_hash} from source machine index {source_machine_index} to dest machine index {dest_machine_index}")
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
			debug(f"Moving object {object_hash_to_migrate} index {object_index} key {object_key_to_migrate}")
			debug(f"Getting from source machine {source_machine.id}")
			object_to_migrate, op_cost = await source_machine.get(object_key_to_migrate)
			cost += op_cost 

			debug(f"Inserting to dest machine {dest_machine.id}")
			insertion_cost, _ = await dest_machine.insert(object_to_migrate)
			cost += insertion_cost
			source_machine.pop(object_key_to_migrate)
			cost += op_costs.move_object_cost(object_to_migrate.size)
		return cost

	def getHash(self, obj):
		hash = hashlib.sha256()
		hash.update(bytes(obj.encode('utf-8')))
		return int(hash.hexdigest(), 16) % self.maximum_capacity, op_costs.compute_hash_cost()

	# Consistent Hashing used to find the relevant machine for the requested object. This will get the first
	# valid machine.
	def getMachineIndex(self, obj_name: str):
		object_hash, hash_cost = self.getHash(obj_name)
		return (bisect_right(self.machine_hashes, object_hash)) % len(self.machines), hash_cost

	async def insertObject(self, req: CacheInsertRequest, counter=0): 
		cost = 0
		new_object_hash, hash_cost = self.getHash(req.object.name + '_' + str(counter))
		cost += hash_cost
		machine_index = (bisect_right(self.machine_hashes, new_object_hash)) % len(self.machines)

		self.db[req.object.name] = req.object

		debug(f"Attempting to insert new object {new_object_hash} to machine index {machine_index}")
		for attempt_count in range(NUM_RETRIES):
			try: 
				insertion_cost, is_new_key = await self.machines[machine_index].insert(req.object, req.background)
				cost += insertion_cost
				if is_new_key:
					self.object_hash_to_key[new_object_hash] = req.object.name
					object_index = bisect_right(self.object_hashes, new_object_hash)
					self.object_hashes.insert(object_index, new_object_hash)
					debug(f"Added new object {new_object_hash} to index {object_index}")
					debug(f"Updated object hashes: {self.object_hashes}")
				# self.replicate_datum(req.object, machine_index, REPLICATION_FACTOR) # TODO: What do we do for replication?
				return cost
			except MemoryError as e: 
				debug(f"Scaling from {len(self.machines)} machines")
				cost += await self.scaleCache(self.scaling_strategy)
				if attempt_count == NUM_RETRIES-1: 
					raise e
			except RuntimeError as e:	
				if attempt_count == NUM_RETRIES-1: 
					raise RuntimeError(f"{e} {attempt_count}")
		return cost

	async def Insert(self, req: CacheInsertRequest):
		cost = 0 

		if self.scaling_strategy == 'replication_factor': 
			if self.replication_factor == '': 
				return ValueError('No replication factor provided')
			cost += await self.insertObject(req, 0)
			# Generate replication factor (RF) number of hashes for the machines and objects
			try:
				task_results = await asyncio.gather(*[self.insertObject(req, i) for i in range(self.replication_factor)])
				cost += sum(cost for cost in task_results)
			except Exception as e:
				raise e

		elif self.scaling_strategy == 'load_aware': 
			obj_hash = self.hash_func(req.object.name)
			self.obj_hitstats[obj_hash].num_total_instances += 1
			cost += await self.insertObject(req)
		else: 
			cost += await self.insertObject(req)

		return cost 

	async def getObject(self, req: CacheGetRequest, counter=0): 
		cost = 0
		machine_index, hash_cost = self.getMachineIndex(req.obj_name + '_' + str(counter))
		cost += hash_cost
		try:
			debug(f"Getting {req.obj_name} from machine index {machine_index} id {self.machines[machine_index].id}")
			cachedObject, get_cost = await self.machines[machine_index].get(req.obj_name)
			cost += get_cost
			return cachedObject, cost

		except RuntimeError as e: # Server is too busy, so try another machine or fail request.
			return None, cost

		except Exception as e:
			debug(f"Exception: {e.__class__} {e}")
			debug(f"Getting {req.obj_name + '_' + str(counter)} from machine index {machine_index} failed")
			return None, cost

	# Add another instance responsible for the object to the cache.
	async def addAnotherInstanceIfHot(self, obj: ObjectToCache):	
		cost = 0
		obj_hash = self.hash_func(obj.name)
		if self.obj_hitstats[obj_hash].num_current_hits >= self.object_load_threshold and self.obj_hitstats[obj_hash].num_total_instances < self.replication_factor:
			self.obj_hitstats[obj_hash].num_total_instances += 1
			cost += await self.insertObject(CacheInsertRequest(obj), self.obj_hitstats[obj_hash].num_total_instances)
		return cost

	async def Get(self, req: CacheGetRequest):
		cost = 0
		counter = 0

		if self.scaling_strategy == 'replication_factor':
			# Attempt to get one of the N replicas of the object. Request hedging (parallel calls to GET) reduces tail latency.
			if self.request_type == 'awaiting':
				try:
					task_results = await asyncio.gather(*[self.getObject(req, counter) for counter in range(self.replication_factor)])
					cost += sum(cost for result, cost in task_results)

					for elt in task_results: 
						if elt != None: 
							return elt[0], cost, 0
				except Exception as e: 
					raise e
			else: 
				for counter in range(self.replication_factor):
					try: 
						task = asyncio.create_task(self.getObject(req, counter))
						task_result = await task

						if task_result[0] != None:
							cost += task_result[1]
							return task_result[0], cost, 0
						else: 
							cost += task_result[1]
					except Exception as e: 
						debug(f"Exception: {e.__class__} {e}")
						debug(f"Getting {req.obj_name + '_' + str(counter)} failed")
						raise e

			# All caches missed.
			cost += op_costs.read_db()
			await self.Insert(CacheInsertRequest(self.db[req.obj_name], True))
			return None, cost, 1

		
		elif self.scaling_strategy == 'horizontal':
			try:
				machine_index, hash_cost = self.getMachineIndex(req.obj_name + "_0")
				cost += hash_cost

				debug(f"Getting {req.obj_name} from machine index {machine_index} id {self.machines[machine_index].id}")
				cachedObject, get_cost = await self.machines[machine_index].get(req.obj_name)
				if cachedObject != None: 
					cost += get_cost
					return cachedObject, cost, 0
				else: 
					cost += op_costs.read_db()
					await self.Insert(CacheInsertRequest(self.db[req.obj_name], True))
					return None, cost, 1

			except RuntimeError as e: # Server too busy. Without replication, we retrieve from DB.
				cost += op_costs.read_db()
				await self.Insert(CacheInsertRequest(self.db[req.obj_name], True))
				return None, cost, 1

			except Exception as e:
				debug(f"Exception: {e.__class__} {e}")
				debug(f"Getting {req.obj_name} from machine index {machine_index} failed")
			raise Exception(f"Getting {req.obj_name} used up max attempts of {self.replication_factor}")

		elif self.scaling_strategy == 'load_aware':
			# Keep track of the load for this particular object.
			obj_hash = self.hash_func(req.obj_name)
			cachedObject = None

			if obj_hash in self.super_hot_obj_to_machine_id:
				if self.request_type == 'awaiting':
					try:
						task_results = await asyncio.gather(*[self.getObject(req, counter) for counter in range(1, self.obj_hitstats[obj_hash].num_total_instances)])

						cost += sum(cost for result, cost in task_results)
						for elt in task_results: 
							if elt != None: 
								return elt[0], cost, 0

					except Exception as e:
						debug(f"Exception: {e.__class__} {e}")
						debug(f"Getting {req.obj_name + '_' + str(counter)} from machine index {machine_index} failed")
						raise e
				else: 
					for counter in range(1, self.obj_hitstats[obj_hash].num_total_instances):
						try: 
							task = asyncio.create_task(self.getObject(req, counter))
							task_result = await task
							if task_result[0] != None:
								cost += task_result[1]
								return task_result[0], cost, 0
							else: 
								cost += task_result[1]

						except Exception as e: 
							debug(f"Exception: {e.__class__} {e}")
							debug(f"Getting {req.obj_name + '_' + str(counter)} failed")

				machine_index = self.machines[self.super_hot_obj_to_machine_id[obj_hash]]
				cachedObject, get_cost = await self.machines[machine_index].get(req.obj_name)
				cost += get_cost
				self.obj_hitstats[obj_hash].num_current_hits -= 1
				if cachedObject == None: 
					cost += op_costs.read_db()
					await self.Insert(CacheInsertRequest(self.db[req.obj_name], True))
					return cachedObject, cost, 1
				return cachedObject, cost, 0

			self.obj_hitstats[obj_hash].num_current_hits += 1

			# Object is super hot, so cache the single worker responsible for it. 
			if self.obj_hitstats[obj_hash].num_current_hits >= 2*self.object_load_threshold:
				machine_index, hash_cost = self.getMachineIndex(req.obj_name + "_0")
				cost += hash_cost
				self.super_hot_obj_to_machine_id[obj_hash] = machine_index
			
			
			if self.request_type == 'awaiting':
				try:
					task_results = await asyncio.gather(*[self.getObject(req, counter) for counter in range(self.obj_hitstats[obj_hash].num_total_instances)])
					cost += sum(cost for result, cost in task_results)
				
					for elt in task_results: 
						if elt != None: 
							return elt[0], cost, 0
				except Exception as e:
					raise e
			else: 
				for counter in range(self.obj_hitstats[obj_hash].num_total_instances):
					try: 
						task = asyncio.create_task(self.getObject(req, counter))
						task_result = await task
						if task_result[0] != None:
							cost += task_result[1]
							return task_result[0], cost, 0
						else: 
							cost += task_result[1]

					except Exception as e: 
						debug(f"Exception: {e.__class__} {e}")
						debug(f"Getting {req.obj_name + '_' + str(counter)} failed")

			# All caches missed.
			cost += op_costs.read_db()
			await self.Insert(CacheInsertRequest(self.db[req.obj_name], True))
			return None, cost, 1

class WorkerCacheServer: 
	def __init__(self, scaling_strategy='horizontal', memory=100, load_threshold=100):
		self.id = uuid.uuid4()
		self.memory = memory 
		self.scaling_strategy = scaling_strategy
		self.objects = {}
		self.num_requests_processing = 0
		self.load_threshold = load_threshold
		self.current_load = 0
		self.LRU = []		
		self.sem = asyncio.Semaphore()

	async def insert(self, obj: ObjectToCache, background: bool=False):

		if self.current_load + op_costs.WRITE_LOAD > self.load_threshold and not background: 
			raise RuntimeError('Server busy')

		if not background: 
			self.current_load += op_costs.WRITE_LOAD

		is_new_key = obj.name not in self.objects
		if not is_new_key:
			async with self.sem:
				self.LRU.remove(obj.name)
				self.LRU.append(obj.name)

		else: # is a new object

			# Determine memory requirements and evict cache if necessary.
			if self.memory - obj.size < 0: 
				if self.scaling_strategy == 'horizontal':
					# Add another node to handle this request.
					debug("Memory error")
					debug(self.memory)
					self.dump()
					raise MemoryError('Machine not large enough')
				else: 
					# Evict least recently used object from cache.
					async with self.sem:
						evicted_obj = self.LRU.pop(0)
						del self.objects[evicted_obj]
			else:
				self.memory -= obj.size	

			async with self.sem:
				self.LRU.append(obj.name)		
				self.objects[obj.name] = obj

			self.dump()

		if not background: 
			self.current_load -= op_costs.WRITE_LOAD
		return op_costs.write_cost(obj.size), is_new_key

	async def get(self, obj_name): 
		cost = 0

		if self.current_load + op_costs.READ_LOAD > self.load_threshold: 
			raise RuntimeError('Server busy')

		self.current_load += op_costs.READ_LOAD

		async with self.sem:
			self.LRU.remove(obj_name)
			self.LRU.append(obj_name)

		self.current_load -= op_costs.READ_LOAD
		self.dump()
		if obj_name in self.objects: 
			cachedObject = self.objects[obj_name]
			cost += op_costs.read_cost(cachedObject.size)
			return cachedObject, cost 
		else:
			return None, ops_cost.cache_miss()
		

	def pop(self, obj_name):
		del self.objects[obj_name]
		debug(f"===========POP============= {obj_name} from {self.id}")
		self.dump()

	def dump(self):
		debug(", ".join(self.objects.keys()))

async def main():
	num_runs = 3
	num_writes = [500, 1000, 2000]
	strats = ['horizontal', 'replication_factor', 'load_aware']
	request_types = ['hedging']

	for mem_size in [1e3]:
		for request_type in request_types: 
			for num_write in num_writes:
				for strat in strats: 
					for replication_factor in [5, 9]:

						costs = []
						num_machines = []
						num_write_errs = []
						num_read_errs = []
						cache_miss_ratios = []
						for _ in range(num_runs):
							num_error = 0

							pcs = PrimaryCacheServer(strat, sys.maxsize, 20, mem_size, replication_factor, request_type=request_type)
							await pcs.initMachines()
							scenario_cost, num_write_err, num_read_err, cache_miss_ratio = await SequentialWriteAndRead(0, pcs, num_write)
					
						costs.append(scenario_cost)
						num_machines.append(len(set(pcs.machines)))
						num_write_errs.append(num_write_err)
						num_read_errs.append(num_read_err)
						cache_miss_ratios.append(cache_miss_ratio)
					print(f"Running strategy: {strat}, #writes: {num_write}, mem_size: {mem_size}, RepFactor: {replication_factor}, request_type: {request_type}")
					print(f"Cost: {mean(costs)}, #machines: {mean(num_machines)}, #write_err: {mean(num_write_errs)}, #read_err: {mean(num_read_errs)}, cache_miss_ratio: {mean(cache_miss_ratios)}")

	# scenario_cost = await BasicWriteAndReadWithNodeFailures(0, pcs)
	# print(f"basic write and read with node failures: {scenario_cost}")

if __name__ == '__main__':
	asyncio.run(main())
