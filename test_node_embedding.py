import networkx as nx
from node2vec import Node2Vec

data = open('dataset_2007.csv', 'r')
next(data, None)

G = nx.parse_edgelist(data, delimiter=',', create_using=nx.Graph,
                      nodetype=int, data=(('counter', int),))

print(
    f"Number of nodes: {G.number_of_nodes()}, number of edges: {G.number_of_edges()}, number of connected components: {nx.number_connected_components(G)}")

node2vec = Node2Vec(G, dimensions=64, walk_length=30, num_walks=200,
                    workers=16)

model = node2vec.fit(window=10, min_count=1)

print(model.wv["1203045"])
# .save_word2vec_format('node2vec_2007.bin', binary=True)
