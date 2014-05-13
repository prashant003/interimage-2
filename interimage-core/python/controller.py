import networkx as nx
import time,sched
#import matplotlib.pyplot as plt
import threading
import thread
from threading import Thread
from threading import Timer
from mortar.api.v2 import API
from mortar.api.v2 import jobs

email = 'example@gmail.com'
api_key = 'EZdoTSiCVBnaCIYEDmtN6nBqrGYaiuH+mct5JAlf'

project_name = 'mortar_example'
cluster_size = 2

#evaluate the edge condition
def eval_edge(G,node1,node2):
	if (G.edge[node1][node2]['condition'] == 'true'):
		return 1
	else:
		return 0

#runs the script inside the node
def eval_node(G,node):
	G.node[node]['running'] = 1
	#time.sleep(G.node[node]['script'])
	#print (node,G.node[node]['script'])
	job_id = jobs.post_job_new_cluster(api, project_name, G.node[node]['script'], cluster_size)
	final_job_status = jobs.block_until_job_complete(api, job_id)
	print (final_job_status)
	update_linked_inputs(G,node)
	G.node[node]['running'] = 0
	G.node[node]['executed'] = 1
	G.node[node]['available'] = 0
	return

#update inputs from successor nodes with output from node
def update_linked_inputs(G,node):
	for suc in G.successors_iter(node):
		if (eval_edge(G,node,suc)==1):
			#update
			G.node[suc]['requests']=G.node[suc]['requests']+1
	return

#evaluate the input data and the requests for executing 
def eval_inputs(G,node):
	#if input data available and
	if (G.node[node]['requests'] == G.in_degree(node)):
		if (G.node[node]['executed']==0):
			G.node[node]['available']=1
	return

#walk the graph recursively
def walk(G,node):
	if (G.node[node]['in'] == G.in_degree(node)): 
		eval_node(G,node,1)
		for suc in G.successors_iter(node):
			if (eval_edge(G,node,suc)==1):
				G.node[suc]['in'] = G.node[suc]['in'] + 1
				walk(G,suc)
	return

#runs available nodes (which have the input data available)
def run(G):
	running=0
	for node in G.nodes():
		eval_inputs(G,node)
		if (G.node[node]['available']==1):
			print 'node ' + str(node) + ' available'
			Thread(target=eval_node, args=(G,node,)).start()
			#t.start()
			#thread.start_new_thread( eval_node, (G,node,1 ))
			#time.sleep(1)
		running = running + G.node[node]['running']
	print 'running ' + str(running) + ' nodes' 
	#if (running == 0):
	#	exit()
	return running
	
#add node to the graph
def addNode(G,node,script):
	G.add_node(node)
	G.node[node]['in'] = 0
	G.node[node]['script'] = script
	G.node[node]['running'] = 0
	G.node[node]['requests'] = 0
	G.node[node]['available'] = 0
	G.node[node]['executed'] = 0
	return

#add edges to the graph
def addEdge(G,fromNode,toNode,label):
	G.add_edge(fromNode,toNode)
	G.edge[fromNode][toNode]['condition'] = label
	return

#create a digraph
G=nx.DiGraph()
	
#add nodes
addNode(G,1,3)
addNode(G,2,3)
addNode(G,3,3)
addNode(G,4,3)
addNode(G,5,3)
addNode(G,6,3)
addNode(G,7,3)

#add edges
addEdge(G,1,2,'true')
addEdge(G,1,3,'true')
addEdge(G,2,4,'true')
addEdge(G,3,4,'true')
addEdge(G,4,5,'true')
addEdge(G,1,6,'true')
addEdge(G,6,5,'true')
addEdge(G,5,7,'false')

#print some info
#print G.nodes(data=True)
#print G.edges(data=True)
#print G.number_of_nodes()
#print G.number_of_edges()
#print G.neighbors(1)
#print G.neighbors(2)
#print G.successors(1)
#print G.predecessors(2)

#show graph rendering
#nx.draw(G)
#plt.show()

#walk the graph
#walk(G,1)
#t = Timer(0.5, lambda: run(G))

api = API(email,api_key)

while run(G)>0:
    time.sleep(2)
