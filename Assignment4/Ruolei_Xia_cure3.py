import numpy as np
import csv
import sys
import numpy as np
import math
from heapq import *

def dist(x, y):
    dis = (x[0] - y[0]) ** 2 + (x[1] - y[1]) ** 2
    return math.sqrt(dis)

def cluster_dist(cluster1, cluster2):
    min = float('inf')
    for i in cluster1:
        for j in cluster2:
            if min > dist(i, j):
                min = dist(i, j)
    return min

def hierarchical(sample): 
    clusters = [[i] for i in sample]
    
    iters = len(sample) - k
    for iter in range(iters):
        min = float("inf")
        for i in range(0, len(clusters) - 1):
            for j in range(i + 1, len(clusters)):
                if min > cluster_dist(clusters[i], clusters[j]):
                    min = cluster_dist(clusters[i], clusters[j])
                    c1 = i
                    c2 = j

        clusters[c1].extend(clusters[c2])
        del clusters[c2]
    
    return clusters

def find_representatives(cluster):
    representatives = []
    representatives.append(list(cluster[0]))
    
    for iter in range(n - 1):
        max = float("-inf")
        for i in cluster:
            if i in representatives:
                continue
            min_dist = float("inf")
            for representative in representatives:
                d = dist(i, representative)
                if d < min_dist:
                    min_dist = d

            if min_dist > max:
                candidate = i
                max = min_dist
        representatives.append(list(candidate)) 
    return representatives

def compute_centroid(cluster):
    x = 0
    y = 0
    for i in cluster:
        x += i[0]
        y += i[1]
    n = len(cluster)
    return (x/n, y/n)

def representative_dist(point, representatives):
    min_dist = float("inf")
    for i in representatives:
        d = dist(point, i)
        if d < min_dist:
            min_dist = d 
    return min_dist

if __name__ == "__main__":
    sample_file = open(sys.argv[1]).readlines()
    data_file = open(sys.argv[2]).readlines()
    k = int(sys.argv[3])
    n = int(sys.argv[4])
    p = float(sys.argv[5])
    output_file = sys.argv[6]
    # data_file = open("/Users/ruolei/Documents/INF553/homework/Assignment4/full_data.txt").readlines()
    # sample_file = open("/Users/ruolei/Documents/INF553/homework/Assignment4/sample_data.txt").readlines()
    # k = 3
    # n = 4
    # p = 0.2

    data = []
    for line in data_file:
        line = line.split(",")
        data.append((float(line[0]), float(line[1])))
    
    sample = []
    for line in sample_file:
        line = line.split(",")
        sample.append((float(line[0]), float(line[1])))
    sample = sorted(sample, key = lambda x:(x[0],x[1]))

    clusters = hierarchical(sample)
    # print(clusters)

    representatives = []
    new_representatives = []
    for cluster in clusters:
        tmp = find_representatives(cluster)
        representatives.append(tmp)
        new_tmp = []
        center = compute_centroid(cluster)
        for representative in tmp:
            delta_x = (center[0] - representative[0]) * p
            delta_y = (center[1] - representative[1]) * p
            new_tmp.append((representative[0] + delta_x, representative[1] + delta_y))
        new_representatives.append(new_tmp)        

    # print("representatives")
    for representative in representatives:
        print(representative)
    # print("new_representatives")
    # print(new_representatives)

    output = []
    for i in data:
        min_dist = float("inf")
        for j in range(k):
            d = representative_dist(i, new_representatives[j])
            if d < min_dist:
                min_dist = d
                c = j
        output.append((i, c))

    output_file = open(output_file, "w")
    for i in output:
        output_file.write(str(i[0][0]) + "," + str(i[0][1]) + "," + str(i[1]) + "\n")