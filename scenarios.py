from utils import *
import asyncio

IS_DEBUG = False

def debug(str):
	if IS_DEBUG:
		print(str)
def get_object_keys(prefix, count):
	return [f"{prefix}_{i}" for i in range(count)]

async def BasicWriteAndRead(scenario_cost, pcs, num_writes=2): 
	debug("Adding objects")
	object_keys = get_object_keys("basic", num_writes)
	insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	for object_key in object_keys:
		got_object, get_cost = await pcs.Get(CacheGetRequest(object_key))
		scenario_cost += get_cost
		assert(got_object.name, object_key)
	
	return scenario_cost, 0

async def BasicWriteAndReadWithNodeFailures(scenario_cost, pcs, num_writes=2, num_failures=1): 
	debug("Adding objects")
	object_keys = get_object_keys("node failure", num_writes)
	insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	for object_key in object_keys:
		got_object, get_cost = await pcs.Get(CacheGetRequest(object_key))
		scenario_cost += get_cost
		assert(got_object.name, object_key)

	for _ in range(num_failures):
		pcs.remove_node()

	debug("Getting objects")
	num_errors = 0
	for object_key in object_keys:
		try:
			got_object, get_cost = await pcs.Get(CacheGetRequest(object_key))
			scenario_cost += get_cost
			assert(got_object.name, object_key)
		except Exception:
			num_errors += 1
	
	return scenario_cost, num_errors

async def LargeHotObject(scenario_cost, pcs, num_writes=1, num_reads=50):
	debug("Adding objects")
	object_keys = get_object_keys("basic", num_writes)
	insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	return_value = await asyncio.gather(*[pcs.Get(CacheGetRequest(object_key)) for object_key in object_keys for _ in range(num_reads)])
	got_object, get_cost = zip(*return_value)
	got_object = list(got_object)
	get_cost = list(get_cost)
	scenario_cost += sum(get_cost)
	
	return scenario_cost, 0