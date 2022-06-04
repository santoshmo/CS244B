from utils import *
import asyncio

IS_DEBUG = False

def debug(str):
	if IS_DEBUG:
		print(str)
def get_object_keys(prefix, count):
	return [f"{prefix}_{i}" for i in range(count)]

async def BasicWriteAndRead(scenario_cost, pcs): 
	debug("Adding first and second object")
	insert_cost = await asyncio.gather(pcs.Insert(CacheInsertRequest(ObjectToCache('first object', 1))), pcs.Insert(CacheInsertRequest(ObjectToCache('second object', 1))))
	scenario_cost += sum(insert_cost)

	debug("Getting first object")
	got_object, get_cost = pcs.Get(CacheGetRequest('first object'))
	scenario_cost += get_cost
	assert(got_object.name, 'first object')
	
	debug("Getting second object")
	got_object_2, get_cost = pcs.Get(CacheGetRequest('second object'))
	scenario_cost += get_cost
	assert(got_object_2.name, 'second object')
	
	return scenario_cost

async def BasicWriteAndReadWithNodeFailures(scenario_cost, pcs, num_writes=2, num_failures=1): 
	debug("Adding objects")
	object_keys = get_object_keys("node failure", num_writes)
	insert_cost = await asyncio.gather(*[pcs.Insert(CacheInsertRequest(ObjectToCache(object_key, 1))) for object_key in object_keys])
	scenario_cost += sum(insert_cost)

	debug("Getting objects")
	for object_key in object_keys:
		got_object, get_cost = pcs.Get(CacheGetRequest(object_key))
		scenario_cost += get_cost
		assert(got_object.name, object_key)

	for _ in range(num_failures):
		pcs.remove_node()

	debug("Getting objects")
	for object_key in object_keys:
		got_object, get_cost = pcs.Get(CacheGetRequest(object_key))
		scenario_cost += get_cost
		assert(got_object.name, object_key)
	
	return scenario_cost