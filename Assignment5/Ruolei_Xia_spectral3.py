import networkx as nx
import csv
import sys
import numpy as np
import math
import numpy as np
from numpy import linalg as LA

def make_laplacian(cluster, G):
    N = len(cluster)
    cluster = sorted(cluster)
    
    A = np.zeros((N, N))
    for i in cluster:
        for j in list(G.adj[i]):
            if j in cluster:
                A[cluster.index(i)][cluster.index(j)] = 1
                A[cluster.index(j)][cluster.index(i)] = 1
    D = np.zeros((N, N))
    for i in range(N):
        D[i][i] = G.degree[cluster[i]]

    L = D - A   
    return L

def modify_graph(pos, neg, G):
    for i in pos:
        for j in list(G.adj[i]):
#             print("vertex")
#             print(i)
#             print("adj")
#             print(G.adj[i])
#             if (i, j) in list(G.edges):
            if j in neg:
                G.remove_edge(i, j)
#             if (j, i) in list(G.edges):
#                 G.remove_edge(j, i)
    return G

if __name__ == "__main__":        
    file = open(sys.argv[1], "r")
    # file = open("/Users/ruolei/Documents/INF553/homework/Assignment5/data/out.ego-facebook", "r")
    # file = open("/Users/ruolei/Documents/INF553/homework/Assignment5/data/test", "r")
    k = int(sys.argv[2])
    clusters = []
    tmp = set()
    G = nx.Graph()
    for line in file.readlines(): 
        line = line.split()
        tmp.add(int(line[0]))
        tmp.add(int(line[1]))
        G.add_node(int(line[0]))
        G.add_node(int(line[1]))
        G.add_edge(int(line[0]), int(line[1]))
    clusters.append(sorted(list(tmp)))

    # print(list(G.nodes))
    # print(list(G.edges))

    for iter in range(k - 1):
        # pick largest cluster 
        max_len = float('-inf')
        max_cluster = []
        for c in clusters:
            if len(c) > max_len:
                max_cluster = c
                max_len = len(c)
        # print(max_cluster)    
    
        L = make_laplacian(max_cluster, G)
        # print(L)
        clusters.remove(max_cluster)
        eigenValues, eigenVectors = np.linalg.eig(L)
        
        # pick the second smallest eigenvalue and corresponding eigenvector
        min1, min2 = float('inf'), float('inf')
        for x in eigenValues:
            if x <= min1:
                min1, min2 = x, min1
            elif x < min2:
                min2 = x

        index = list(eigenValues).index(min2)
        # print(index)
        # print(eigenVectors[:, index])
        second_eigenvector = eigenVectors[:, index]
        
        # split the eigenvector at 0 into 2 clusters
        tmp = []
        pos = []
        neg = []
        for i in range(len(second_eigenvector)):
            if second_eigenvector[i] >= 0:
                pos.append(max_cluster[i])
            else:
                neg.append(max_cluster[i])
        clusters.append(sorted(pos))
        clusters.append(sorted(neg))
#         print(pos)
#         print(neg)
#         print(max_cluster)
        G = modify_graph(pos, neg, G)
        
        # print(clusters)

    res = []
    for cluster in clusters:
        cluster = sorted(cluster)
        # print(len(cluster))
        res.append(",".join(str(i) for i in cluster))
    # print(res)
    output = open(sys.argv[3], "w")
    for i in res:
        output.write(i + "\n")
