from pyspark import SparkContext
import os, sys

def hash(i, j):
    return (a * i + b * j) % N

def pass1(baskets):
    for basket in baskets:
        for item in basket:
            if item in item_count_table.keys():
                item_count_table[item] += 1
            else:
                item_count_table[item] = 1
    
        for i in range(0, len(basket)):
            for j in range(i + 1, len(basket)):
                pair = (basket[i], basket[j])
                bucket_num = hash(basket[i], basket[j])
                if bucket_num in hash_table.keys():
                    hash_table[bucket_num] += 1
                else:
                    hash_table[bucket_num] = 1
    
    for key in item_count_table.keys():
        if item_count_table[key] >= s:
            frequent_items.append(int(key))
    for key in hash_table.keys():
        if hash_table[key] >= s:
            frequent_buckets.append(key)
    # print("111")
    # print(item_count_table)
    # print(hash_table)
    # print(frequent_items)
    # print(frequent_buckets)
    
def pass2(baskets):
    for basket in baskets:
        for i in range(0, len(basket)):
            for j in range(i + 1, len(basket)):
                pair = (basket[i], basket[j])
                bucket_num = hash(basket[i], basket[j])
                if basket[i] in frequent_items and basket[j] in frequent_items and bucket_num in frequent_buckets:
                    if pair in pair_count_table.keys():
                        pair_count_table[pair] += 1
                    else:
                        pair_count_table[pair] = 1
                
    for key in pair_count_table.keys():
        if pair_count_table[key] >= s:
            frequent_pairs.append(key)
    # print("222")        
    # print(frequent_pairs)
    # print(pair_count_table)

def find_candidate_pairs(baskets):
    for basket in baskets:
        # basket = [int(item) for item in basket]
        for i in range(0, len(basket)):
            for j in range(i + 1, len(basket)):
                pair = (basket[i], basket[j])
                bucket_num = hash(basket[i], basket[j])
                if basket[i] in frequent_items and basket[j] in frequent_items and bucket_num not in frequent_buckets and pair not in candidate_pairs:
                    candidate_pairs.append(pair)
    # print("333")
    # print(candidate_pairs)               

if __name__ == '__main__':
    sc = SparkContext(appName = "inf553")
    file = sc.textFile(sys.argv[1])
    a = int(sys.argv[2])
    b = int(sys.argv[3])
    N = int(sys.argv[4])
    s = int(sys.argv[5])
    output_dir = sys.argv[6]

    item_count_table = {}
    pair_count_table = {}
    hash_table = {}
    frequent_items = []
    frequent_pairs = []
    candidate_pairs = []
    frequent_buckets = []

    baskets = file.map(lambda line: line.split(",")).map(lambda x: [int(i) for i in x]).collect()
    
    pass1(baskets)
    pass2(baskets)
    find_candidate_pairs(baskets)
    
    sorted_frequent_items = sorted(frequent_items)
    sorted_frequent_pairs = sorted(frequent_pairs)
    sorted_candidate_pairs = sorted(candidate_pairs)
    
    # output1 = open("/Users/ruolei/Desktop/output/frequentset.txt", "w")
    if os.path.exists(output_dir) == False: 
        os.mkdir(output_dir)
    output1 = open("%s/frequentset.txt" %output_dir, "w")
    output2 = open("%s/candidates.txt" %output_dir, "w")

    for i in sorted_frequent_items:
        output1.write(str(i) + "\n")
    for i in sorted_frequent_pairs:
        output1.write("(" + str(i[0]) + "," + str(i[1]) + ")" + "\n")

    for i in sorted_candidate_pairs:
        output2.write("(" + str(i[0]) + "," + str(i[1]) + ")" + "\n")
    false_positive_rate = len(frequent_buckets) / N
    print("False Positive Rate: %.3f" % false_positive_rate)
    