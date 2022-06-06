READ_LOAD = 1
WRITE_LOAD = 2

def cache_miss(): 
	return 1

def read_db(): 
	return 1000

def write_cost(object_size): 
	return object_size

def read_cost(object_size): 
	return object_size

def compute_hash_cost(): 
	return 5

def add_node_cost(): 
	return 200

def move_object_cost(object_size): 
	# TODO: Scale it to similar factor of others, or make them all a function of object size
	# Networking + Read + Write of an object
	return 2*object_size + 1.5*object_size 