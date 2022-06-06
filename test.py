import asyncio
from datetime import datetime
import time

class Instance:
	def __init__(self, capacity):
		# self.cond = asyncio.Semaphore(capacity)
		self.counter = 0

async def add(instance): 
	cur_time = datetime.now()
	if instance.counter >= 5: 
		return 
	else: 
		instance.counter += 1

async def return_two(x):
	return x, x+1
async def main():
	task = asyncio.create_task(return_two(1))
	task_result = await task
	print(task_result[0])

	# instance = Instance(2)
	# await asyncio.gather(*[add(instance) for _ in range(10)])
	# print(instance.counter)


asyncio.run(main())


