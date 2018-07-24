from __future__ import print_function

import re
import os, sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    for url in urls:
        yield (url, rank)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


def update_auths(hubs):
    auths = auth_links.join(hubs) \
            .flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])) \
            .reduceByKey(lambda x, y: x + y)
    auths.cache()
    return auths


def update_hubs(auths):
    hubs = hub_links.join(auths) \
            .flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])) \
            .reduceByKey(lambda x, y: x + y)
    hubs.cache()
    return hubs


def normalize(rdd):
    max_value = rdd.sortBy(lambda x: x[1], ascending=False).first()
    norm = rdd.map(lambda x: (x[0], x[1] / max_value[1]))
    norm.cache()
    return norm


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # lines = spark.read.text("/Users/ruolei/Documents/INF553/homework/Assignment5/test.txt").rdd.map(lambda r: r[0])
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    # print(lines.collect())

    # Loads all URLs from input file and initialize their neighbors.
    auth_links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    hub_links = lines.map(lambda urls: parseNeighbors(urls)).map(lambda x: (x[1], x[0])).distinct().groupByKey().cache()
    # print(auth_links.mapValues(list).collect())
    # print(hub_links.mapValues(list).collect())

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    hubs = auth_links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    print(hubs.collect())

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        auths = update_auths(hubs)
        auths = normalize(auths)
        hubs = update_hubs(auths)
        hubs = normalize(hubs)
    
    # print(auths.collect())
    # print(hubs.collect())
    sorted_auths = sorted(auths.collect(), key = lambda x: int(x[0]))
    sorted_hubs = sorted(hubs.collect(), key = lambda x: int(x[0]))

    output_dir = sys.argv[3]
    if os.path.exists(output_dir) == False: 
        os.mkdir(output_dir)
    output1 = open("%s/authority.txt" %output_dir, "w")
    output2 = open("%s/hub.txt" %output_dir, "w")


    for i in sorted_auths:
        auth = "%.5f" % i[1]
        output1.write(i[0] + "," + str(auth) + "\n")

    for i in sorted_hubs:
        hub = "%.5f" % i[1]
        output2.write(i[0] + "," + str(hub) + "\n")



