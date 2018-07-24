from pyspark import SparkContext
import sys
from operator import add
import re

if __name__ == '__main__':
    def process(data):
        data = data.lower().replace("-", "").replace("'", "")
        data = re.sub(r'[^0-9a-zA-Z]', " ", data)
        data = ' '.join(data.split())
        data = data.strip()
        return data

    sc = SparkContext(appName="inf553")
    lines = sc.textFile(sys.argv[1])
    header = lines.first()

    results = lines.filter(lambda x: x != header) \
        .map(lambda x: x.split(",")) \
        .map(lambda p: (p[3], int(p[18]))) \
        .map(lambda p: (process(p[0]), p[1])) \
        .filter(lambda p: p[0] != '') \
        .map(lambda p: (p[0],(p[1],1))) \
        .reduceByKey(lambda U, x: (U[0]+x[0], U[1]+x[1])) \
        .mapValues(lambda x: (x[1], ("%.3f" % ((x[0]*1.0)/x[1]))))\
        .sortByKey()\
        .map(lambda x: x[0] + "\t" + str(x[1][0]) + "\t" + str( x[1][1]))

    results.coalesce(1).saveAsTextFile(sys.argv[2])

