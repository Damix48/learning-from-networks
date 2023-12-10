import networkx as nx
import matplotlib.pyplot as plt
import time

import scipy.stats as stats

data = open('dataset_2008.csv', 'r')
next(data, None)

G = nx.parse_edgelist(data, delimiter=',', create_using=nx.Graph,
                      nodetype=int, data=(('counter', int),))

G = G.subgraph(max(nx.connected_components(G), key=len)).copy()

p = G.number_of_edges() / (G.number_of_nodes() * (G.number_of_nodes() - 1) / 2)

print(
    f"Number of nodes: {G.number_of_nodes()}, number of edges: {G.number_of_edges()}, number of connected components: {nx.number_connected_components(G)}")

# random_graph = nx.fast_gnp_random_graph(G.number_of_nodes(), p)
# random_graph = nx.erdos_renyi_graph(G.number_of_nodes(), p)
# random_graph = nx.newman_watts_strogatz_graph(G.number_of_nodes(), 5, p)

data = open('dataset_all.csv', 'r')
next(data, None)

random_graph = nx.parse_edgelist(data, delimiter=',', create_using=nx.Graph,
                                 nodetype=int, data=(('counter', int),))

random_graph = random_graph.subgraph(
    max(nx.connected_components(random_graph), key=len)).copy()

print(
    f"Number of nodes: {random_graph.number_of_nodes()}, number of edges: {random_graph.number_of_edges()}, number of connected components: {nx.number_connected_components(random_graph)}")


cc_G = nx.algorithms.approximation.clustering_coefficient.average_clustering(G)
cc_random_graph = nx.algorithms.approximation.clustering_coefficient.average_clustering(
    random_graph)

t_G = nx.algorithms.cluster.triangles(G)
t_random_graph = nx.algorithms.cluster.triangles(random_graph)

print(cc_G)
print(cc_random_graph)

print("---")

n_T_G = 0
for t in t_G:
    n_T_G += t

n_t_random_graph = 0
for t in t_random_graph:
    n_t_random_graph += t

print(n_T_G)
print(n_t_random_graph)

# plt.figure(1)
# nx.draw_networkx(G, with_labels=False, node_size=5)

# plt.figure(2)
# nx.draw_networkx(random_graph, with_labels=False, node_size=5)

# plt.show()


# # nx.draw_networkx(G, with_labels=False, node_size=5)
# # nx.draw_networkx_edges(G, pos=nx.planar_layout(G))
# # plt.show()

# # Network info
# print(f"Number of nodes: {G.number_of_nodes()}")
# print(f"Number of edges: {G.number_of_edges()}")
# start = time.time()
# print(
#     f"Average clustering coefficient: {nx.average_clustering(G)}, taken {time.time() - start} seconds")
# start = time.time()
# # print(
# #     f"Average shortest path length: {nx.average_shortest_path_length(G)}, taken {time.time() - start} seconds")
# start = time.time()
# out = nx.degree_centrality(G)
# print(f"Degree centrality taken {time.time() - start} seconds")
# start = time.time()
# triangles = nx.triangles(G)
# print(f"Triangles taken {time.time() - start} seconds")

# number_c = 0
# num = 0
# for i in nx.connected_components(G):
#     number_c += 1
#     num += len(i)

# print(f"Number of connected components: {number_c}")
# print(f"Number of nodes in connected components: {num}")

# with open('actor_centrality.csv', 'w') as f:
#     for k, v in out.items():
#         f.write(str(k) + ',' + str(v) + '\n')
