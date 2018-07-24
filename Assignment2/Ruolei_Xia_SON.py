# -*- coding: utf-8 -*-
# @Author: ruolei
# @Date:   2018-03-01 20:37:08
# @Last Modified by:   ruolei
# @Last Modified time: 2018-03-02 13:23:22
import sys
from pyspark import SparkContext
from itertools import combinations
import collections

def pass1(baskets):
    baskets = list(baskets)
    # print(baskets)
    local_support = support_ratio * len(baskets)
    local_count_table = collections.defaultdict(int)

    backet_set = [set(x) for x in baskets]
    item_set = set().union(*backet_set)
    for item in item_set:
        for basket in baskets:
            local_count_table[item] += 1
            if local_count_table[item] >= local_support:
                candidates.append(item)
                break

    num_items_in_tuple = 2
    sorted_local_frequent = sorted(candidates)

        
    while(True):
        tuple_combinations = combinations(sorted_local_frequent, num_items_in_tuple)
        filtered_tuple_combinations = []
        local_frequent = []


        if num_items_in_tuple > 2:
            for tuple_combination in tuple_combinations:
                for each in last_local_frequent:
                    if set(each).issubset(set(tuple_combination)):
                        filtered_tuple_combinations.append(tuple_combination)
                        break
        else:
            filtered_tuple_combinations = tuple_combinations

        for tuple_combination in filtered_tuple_combinations:
            for basket in baskets: 
                if set(tuple_combination).issubset(set(basket)):
                    local_count_table[tuple_combination] += 1
                    if local_count_table[tuple_combination] >= local_support:
                        local_frequent.append(tuple_combination)
                        candidates.append(tuple_combination)
                        break

        if(len(local_frequent) == 0): 
            break
        sorted_local_frequent = set().union(*local_frequent)
        last_local_frequent = local_frequent 
        num_items_in_tuple += 1

    return candidates

def pass2(baskets):
    baskets = list(baskets)
    count_table = collections.defaultdict(int)
    for candidate in candidates:
        for basket in baskets:
            if isinstance(candidate,int):
                if candidate in basket:
                    count_table[candidate] += 1
            else:
                if(set(candidate).issubset(basket)):
                    count_table[candidate] += 1
    # print("-----------count_table-----------")
    # print(count_table)
    return count_table.items()

if __name__ == '__main__':
    sc = SparkContext(appName = "inf553")
    file = sc.textFile(sys.argv[1])
    # file = sc.textFile("/Users/ruolei/Downloads/hw2/baskets.txt")
    # file = sc.textFile("/Users/ruolei/Downloads/HW2_INF553/test_cases_examples/support_ratio_0.3/baskets.txt")
    baskets = file.map(lambda line: line.split(',')).map(lambda x: [int(i) for i in x])
    support_ratio = float(sys.argv[2])
    
    global_count_table = collections.defaultdict(int)
    
    candidates = []
    candidates = baskets.mapPartitions(pass1).distinct().collect()

    num_baskets = baskets.map(lambda x : 1).reduce(lambda x, y: x + y)
    global_support = support_ratio * num_baskets

    result = baskets.mapPartitions(pass2).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= global_support).map(lambda x: x[0]).collect()
    # print(result)

    singletons = []
    tuples = []
    for i in result:
        if isinstance(i,int):
            singletons.append(i)
        else:
            tuples.append(i)

    sorted_singletons = sorted(singletons)
    sorted_tuples = sorted(tuples, key = lambda x:(len(x),x))

    output = open(sys.argv[3], "w")
    # output = open("%s/frequentset.txt" %output_dir, "w")
    for i in sorted_singletons:
        output.write(str(i) + "\n")
    for sorted_tuple in sorted_tuples:
        string = "("
        for each in sorted_tuple[0:-1]:
            string +=  str(each) + ","
        string += str(sorted_tuple[-1]) + ")"    
        output.write(string + "\n")
    