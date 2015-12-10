import zmq as zmq
import pprint
from docopt import docopt, DocoptExit
import time

VERSION_STRING = "senmo py-router 0.0.1"
CMD_REF = """
Usage:
  > map (sub|pull) <port_in> (push|pub) <port_out> [--topic|-t (<topics>)...] [--prefix|-p <prefix>]
  > unmap <port_in> <port_out> [--topic|-t (<topics>)...] [--all-topics]
  > unmap group (input|output) <port>
  > print
  > help
  > version

Options:
	-t (<topic>)..., --topic (<topic>)...  list SUB topics to act on
	-p <prefix>, --prefix <prefix>         PUB topic to prefix each message
	--all-topics                           unmap all topics for the port pair
"""

CMD_REF = VERSION_STRING + CMD_REF

CONTROL_PORT = 6000
HOSTNAME = "localhost"

port_map = {}
input_map = {}
output_map = {}

input_map_set = set()

context = zmq.Context()

control_socket = context.socket(zmq.REP)
control_socket.bind("tcp://*:6000")

poller = zmq.Poller()

poller.register(control_socket, zmq.POLLIN)

def parse_cmd_arr(cmd_dict):
	print (cmd_dict)
	if cmd_dict['map'] or cmd_dict['unmap']:
		# Nice little functional swap to an array of tuples
		# marginally vestigial from prevously allowed multiple inputs
		port_tuple = (cmd_dict['<port_in>'],cmd_dict['<port_out>'])

		# If we are mapping we need to actually parse the arguments
		if cmd_dict['map']:
			in_mode = ""
			if cmd_dict['pull']:
				in_mode = 'pull'
			elif cmd_dict['sub']:
				in_mode = 'sub'

			out_mode = ""
			if cmd_dict['push']:
				out_mode = 'push'
			elif cmd_dict['pub']:
				out_mode = 'pub'

			prefix = cmd_dict['<prefix>']
			if prefix is None and out_mode is 'pub':
				prefix = "flow-"+cmd_dict['<port_out>']

			cmd_map(port_tuple, in_mode, out_mode, cmd_dict['<topics>'], prefix)
		# if unmapping tuples alone are fine
		else:
			if cmd_dict["--all-topics"]:
				cmd_unmap_all_topics(port_tuple)
			elif cmd_dict['input']:
				cmd_unmap_input(cmd_dict['<port>'])
			else:
				cmd_unmap(port_tuple,cmd_dict['<topics>'])
	elif cmd_dict['print']:
		cmd_print()
	elif cmd_dict['help']:
		cmd_help()
	elif cmd_dict['version']:
		cmd_version()
	else:
		cmd_help()

def cmd_map(port_tuple, in_mode, out_mode, topics, prefix):
	output_string = ""
	# At some point we can remove these loops
	if add_mapping(port_tuple[0],port_tuple[1], in_mode, out_mode, topics, prefix):
		output_string += "mapped " + str(port_tuple[0]) +":"+str(port_tuple[1]+". ")
	else:
		output_string += ("mapping failed, output port in use. ")

	control_socket.send_string(output_string)

def cmd_unmap(port_tuple, topics):
	output_string = ""
	if remove_mapping(port_tuple[0], port_tuple[1], topics=topics):
		output_string += ("unmapped " + str(port_tuple[0]) +":"+str(port_tuple[1]) + ". ")
	else:
		output_string += "failed to unmap"
	control_socket.send_string(output_string)

def cmd_unmap_all_topics(port_tuple):
	output_string = ""
	if remove_mapping_for_all_topics(port_tuple[0], port_tuple[1]):
		output_string += ("unmapped all associated topics")
	else:
		output_string += "failed to unmap all topics"
	control_socket.send_string(output_string)

def cmd_unmap_input(port):
	output_string = ""
	if remove_mapping_by_input(port):
		output_string += ("unmapped input port: "+str(port))
	else:
		output_string += "failed to unmap input port: "+str(port)
	control_socket.send_string(output_string)

def cmd_print():
	control_socket.send_string(pprint.pformat((port_map,input_map,output_map), compact=True))

def cmd_help():
	control_socket.send_string(CMD_REF)

def cmd_error():
	control_socket.send_string("Command not recognized. Type help for help")

def cmd_version():
	control_socket.send_string(VERSION_STRING)

def remove_mapping(port_in, port_out, topics=[]):
	# get input combo
	try:
		input_mode = port_map[port_in][0]
	except KeyError:
		return False
	if input_mode == 'sub':
		input_socket = get_socket_with_topics(port_in, topics)
		if input_socket == None:
			return False
	else:
		input_socket = port_map[port_in][1][0]

	safe_del_port_out(port_out,input_socket=input_socket)

	safe_del_port_in(port_in,input_socket, topics)

	return True

def remove_mapping_for_all_topics(port_in, port_out):
	try:
		input_mode = port_map[port_in][0]
	except KeyError:
		return False
	if input_mode == 'sub':
		# get all possible topics
		print("starting to work on topics")
		input_topics = [t for (s, t) in port_map[port_in][1:]]
		for topic in input_topics:
			print("working on topic: "+str(topic))
			remove_mapping(port_in, port_out, topics=topic)
		return True
	else:
		return False

def remove_mapping_by_input(port_in):
	# we use a slice up here which intrinsically copies so we do not have to worry about creating a new list
	for s, t in port_map[port_in][1:]:
		# need to make a new list here because safe_del edits the base list as it runs
		outputs = list(input_map[s])
		for output_port in outputs:
			safe_del_port_out(output_port, s)
		
		safe_del_port_in(port_in,s,t)

	return True

def safe_del_port_in(port_in, input_socket, topics):

	# check length of input map list
	# if zero we know the last mapping of this input has been removed and we should remove the whole thing
	if len(input_map[input_socket]) == 0:
		# Must unregister to avoid high cpu usage
		poller.unregister(input_socket)
		input_socket.close()
		del input_map[input_socket]
		
	if input_socket not in input_map:
		port_map[port_in].remove((input_socket,topics))

	# if the port_map entry for the port is one there must only be the mode remaining and we may delete
	if len(port_map[port_in]) == 1:
		del port_map[port_in]

def safe_del_port_out(port_out, input_socket=None):
	# remove from input output map
	if input_socket:
		if port_out in input_map[input_socket]:
			input_map[input_socket].remove(port_out)
	# now scan the remainder of the input map to find any other uses of the output socket
	found = False
	for key, val in input_map.items():
		# if we find an instance stop
		if port_out in val:
			found = True
			break

	# if we did not find any other uses remove the output map instance
	if not found and port_out != 'sinkhole' :
		output_map[port_out][0].close()
		del output_map[port_out]

def add_mapping(port_in, port_out, in_mode, out_mode, topics, prefix):
	# If we have not already created an output socket for this output create one now to ensure we can bind succesfully
	if port_out not in output_map and port_out != 'sinkhole':
		if out_mode == 'pub':
			socket_out = context.socket(zmq.PUB)
		elif out_mode == 'push':
			socket_out = context.socket(zmq.PUSH)
		else:
			# Default case
			socket_out = context.socket(zmq.PUSH)

		try:
			socket_out.bind("tcp://*:"+str(port_out))
		except zmq.error.ZMQError:
			print("failed to bind")
			return False
		output_map[port_out] = (socket_out,out_mode,prefix)

	# Check if port exists in map and add if needed
	if port_in not in port_map:
		# create a new list for mapping items where the first element is in_mode
		port_map[port_in] = [in_mode]
		if in_mode == 'pull':
			socket_in = context.socket(zmq.PULL)
		elif in_mode == 'sub':
			socket_in = create_socket_from_topics(topics)
		else:
			# Default case
			socket_in = context.socket(zmq.PULL)
		
		setup_input_socket(HOSTNAME,port_in,topics,socket_in)
	else:
		# compare modes
		# zeroth index contains the mode
		if port_map[port_in][0] == in_mode:
			# if modes match and the mode is sub then do stuff
			# If the mode is anything else do nothing
			if in_mode == 'sub':
				# check for matching topics --> tuple format is (socket, [topics])
				# see if we can get a socket with the matching topics
				# if we can it already exists and thus we should not add
				if get_socket_with_topics(port_in,topics) == None:
					new_socket = create_socket_from_topics(topics)
					setup_input_socket(HOSTNAME,port_in,topics,new_socket)

		else:
			# do some error handling
			print("modes do not match")
			# remove output mapping
			safe_del_port_out(port_out)
			return False


	# Get socket from port (this function should never return none within this block as we should have just defined a matching socket. If we fail below this line something has gone very wrong. This function may also throw an error...if this happens something has also gone very wrong)
	input_socket = get_socket_with_topics(port_in, topics)

	# Check and setup input map
	if input_socket not in input_map:
		input_map[input_socket] = []
		input_map_set.add(input_socket)

	if port_out not in input_map[input_socket]:
		input_map[input_socket].append(port_out)

	return True

def setup_input_socket(hostname, port_in, topics, socket):
	socket.connect("tcp://"+hostname+":"+str(port_in))
	port_map[port_in].append((socket, topics))
	poller.register(socket, zmq.POLLIN)

def get_socket_with_topics(port_in, topics):
	socket = [s for (s,t) in port_map[port_in][1:] if t == topics]
	if len(socket) > 1:
		print("dumping maps")
		print(pprint.pformat((port_map,input_map,output_map), compact=True))
		raise Exception("port_map is invalid: there are more than one input port with the same set of topics")
	elif len(socket) == 0:
		return None
	else:
		return socket[0]

def create_socket_from_topics(topics):
	socket = context.socket(zmq.SUB)
	# Do all the topics
	for topic in topics:
		socket.setsockopt_string(zmq.SUBSCRIBE, topic)
	# if there were no topics forward all topics
	if len(topics) == 0:
		socket.setsockopt_string(zmq.SUBSCRIBE, '')

	return socket

def send_string_to_sockets(input_socket, string):
	# get ports to send to
	port_list = input_map[input_socket]

	for port in port_list:
		# don't send if the output port is the sinkhole
		if port != 'sinkhole':
			socket_out = output_map[port][0]
			prefix = output_map[port][2]
			if prefix is None:
				prefix = ''
			try:
				socket_out.send_string("%s %s" % (prefix, string), zmq.NOBLOCK)
			except:
				if output_map[port][1] == 'push':
					print("No consumer active @" + port +" "+str(time.time()))

while True:
	socks = dict(poller.poll(100))

	if control_socket in socks and socks[control_socket] == zmq.POLLIN:
		cmd = control_socket.recv_string().split(" ")
		try:
			parsed_cmd_arr = docopt(CMD_REF,argv=cmd,help=False,version=None, options_first=False)
			parse_cmd_arr(parsed_cmd_arr)
		except DocoptExit:
			cmd_error()
		
		
	update_socks = input_map_set.intersection(set(socks.keys()))

	for update_sock in update_socks:
		if socks[update_sock] == zmq.POLLIN:
			data = update_sock.recv_string()
			send_string_to_sockets(update_sock, data)


