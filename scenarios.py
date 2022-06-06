from utils import *
import asyncio

IS_DEBUG = False
import pdb

def debug(str):
	if IS_DEBUG:
		print(str)
def get_object_keys(prefix, count):
	return [f"{prefix}_{i}" for i in range(count)]

async def SequentialWriteAndRead(scenario_cost, pcs, num_writes=2): 
	num_gets = 0
	db_hits = 0
	debug("Adding objects")
	object_keys = get_object_keys("basic", num_writes)
	try: 
		insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	except Exception as e:
		# scenario_cost += insert_cost
		return scenario_cost, 1, 0, 1.0

	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	for object_key in object_keys:
		try:
			got_object, get_cost, db_hit = await pcs.Get(CacheGetRequest(object_key))
		except Exception as e:
			# scenario_cost += get_cost
			return scenario_cost, 0, 1, 1.0

		scenario_cost += get_cost
		db_hits += db_hit
		num_gets += 1
		# assert(got_object.name, object_key)
	
	return scenario_cost, 0, 0, float(db_hits/num_gets)

async def SequentialWriteAndReadWithNodeFailures(scenario_cost, pcs, num_writes=2, num_failures=1): 
	debug("Adding objects")
	num_gets = 0
	db_hits = 0
	object_keys = get_object_keys("node failure", num_writes)
	try:
		insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	except Exception as e:
		# scenario_cost += insert_cost
		return scenario_cost, 1, 0, 1.0

	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	for object_key in object_keys:
		try: 
			got_object, get_cost, db_hit = await pcs.Get(CacheGetRequest(object_key))
		except Exception as e: 
			return scenario_cost, 0, 1, 1.0

		scenario_cost += get_cost
		db_hits += db_hit
		num_gets += 1
		# assert(got_object.name, object_key)

	for _ in range(num_failures):
		pcs.remove_node()

	debug("Getting objects")
	num_errors = 0
	for object_key in object_keys:
		try:
			got_object, get_cost, db_hit = await pcs.Get(CacheGetRequest(object_key))
			scenario_cost += get_cost
			# assert(got_object.name, object_key)
		except Exception as e:
			return scenario_cost, 0, 1, 1.0
		
		db_hits += db_hit
		num_gets += 1
	
	return scenario_cost, 0, 0, float(db_hits/num_gets)