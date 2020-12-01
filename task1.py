import binascii
import csv
import json
import random
import sys
import time
from pyspark import SparkContext

def perform_hash_calculation(a_coeff, b_coeff, m, x):

    result = []
    
    for i in range(len(a_coeff)):
        result.append(((a_coeff[i]*x + b_coeff[i])%m))
    return result

def perform_test(a_coeff, b_coeff, m, x, bloom_bit):

    res_list = perform_hash_calculation(a_coeff, b_coeff, m, x)
    
    for val in res_list:
        if bloom_bit[val] != 1:
            return 'F'
            
    return 'T'

#spark-submit task1.py <first_json_path> <second_json_path> <output_ filename>

if __name__ == "__main__":

    start = time.time()
    
    first_path = sys.argv[1]
    second_path = sys.argv[2]
    output_path = sys.argv[3]
    
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    
    train_rdd = sc.textFile(first_path).map(lambda x: json.loads(x)).map(lambda y: y['name']).distinct().map(lambda s: int(binascii.hexlify(s.encode("utf8")), 16))
    test_rdd = sc.textFile(second_path).map(lambda x: json.loads(x)).map(lambda y: y['name']).map(lambda s: int(binascii.hexlify(s.encode("utf8")), 16))
    
    hashes_count = 5
    m = train_rdd.count()*50
    
    a_coeff = random.sample([i for i in range(1, m+1)], hashes_count)
    b_coeff = random.sample([i for i in range(0, m)], hashes_count)
    
    bloom_bit = [0 for i in range(m)]
    
    cal_res = train_rdd.flatMap(lambda x: perform_hash_calculation(a_coeff, b_coeff, m, x)).distinct().collect()
    
    for idx in cal_res:
        bloom_bit[idx] = 1
        
    test_result = test_rdd.map(lambda x: perform_test(a_coeff, b_coeff, m, x, bloom_bit)).collect()
    
    print(len(test_result))
    with open(output_path, "w") as fp:
        wp = csv.writer(fp, delimiter = ' ')
        wp.writerow(test_result)
    
    print("Duration:", time.time()-start)
    