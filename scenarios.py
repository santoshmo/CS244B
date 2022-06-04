from utils import *
import asyncio

async def BasicWriteAndRead(scenario_cost, pcs): 
	print("Adding first and second object")
	insert_cost = await asyncio.gather(pcs.Insert(CacheInsertRequest(ObjectToCache('first object', 1))), pcs.Insert(CacheInsertRequest(ObjectToCache('second object', 1))))
	scenario_cost += sum(insert_cost)

	print("Getting first object")
	got_object, get_cost = pcs.Get(CacheGetRequest('first object'))
	scenario_cost += get_cost
	assert(got_object.name, 'first object')
	
	print("Getting second object")
	got_object_2, get_cost = pcs.Get(CacheGetRequest('second object'))
	scenario_cost += get_cost
	assert(got_object_2.name, 'second object')
	
	return scenario_cost