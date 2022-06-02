def write_cost(): 
	return 50

def read_cost(): 
	return 10

def compute_hash_cost(): 
	return 5

def add_node_cost(): 
	return 300

def move_object_cost(object_size): 
	# TODO: Scale it to similar factor of others, or make them all a function of object size
	# Networking + Read + Write of an object
	return 2*object_size + 1.5*object_size 