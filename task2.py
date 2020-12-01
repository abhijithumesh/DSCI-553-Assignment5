import binascii
import csv
import json
import random
import sys
import time
import math

from datetime import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

start = time.time()

no_of_hash_fns = 500

a_coeff = [random.randint(1, 10000) for i in range(no_of_hash_fns)]
b_coeff = [random.randint(1, 10000) for i in range(no_of_hash_fns)]
m_coeff = [random.randint(1, 10000) for i in range(no_of_hash_fns)]

def median(list_input):

    sort_list = sorted(list_input)
    no = len(sort_list)
    
    median_val = no//2
    
    if no %2 !=0:
        return sort_list[median_val]
    else:
        val1 = sort_list[median_val]
        val2 = sort_list[median_val-1]
        
        return (val1+val2) / 2
        
def apply_hash_function(hash_list):

    nested_hash_values = []
    result = [0 for i in range(no_of_hash_fns)]
    
    for ctr in range(no_of_hash_fns):
    
        hash_value = []
        for state in hash_list:
            hash_value.append("{0:b}".format(((a_coeff[ctr]*state+b_coeff[ctr]) % m_coeff[ctr])))
            
        nested_hash_values.append(hash_value[:])
        
    for itr in range(len(nested_hash_values)):
    
        count_zero = []
        
        for value in nested_hash_values[itr]:
            count = 0
            idx = 0
            
            while value[len(value)-idx-1] == '0' and idx < len(value):
                count += 1
                idx += 1
            
            count_zero.append(count)
        
        result[itr] = pow(2, max(count_zero))
        
    return result


def calculate_distinct(input_rdd):

    curr_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    uniq = set()
    hash_list = []
    
    states = input_rdd.collect()
    
    for state in states:
        state = json.loads(state)['state']
        if state == "":
            continue
        else:
            uniq.add(state)
            hash_list.append(int(binascii.hexlify(state.encode('utf8')),16))
            
    rst = apply_hash_function(hash_list)
    
    rst = sorted(rst)
    
    
    
    
    result = [str(curr_time), str(len(uniq)), str(estimation)]
    
    print(result + [(abs(len(uniq)-estimation)/len(uniq))])
    
    with open(file_name, "a") as fp:
        w_fp = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        w_fp.writerow(result)

    
    
port = int(sys.argv[1])
file_name = sys.argv[2]

sc = SparkContext()
sc.setLogLevel("ERROR")

window_length = 30
sliding_duration = 10

ssc = StreamingContext(sc, 5)
textStream = ssc.socketTextStream("localhost", port)

text = textStream.window(windowDuration=window_length, slideDuration=sliding_duration)

with open(file_name, "w") as fp:
    header = ["Time", "Gound Truth", "Estimation"]
    w_fp = csv.writer(fp)
    w_fp.writerow(header)
    
text.foreachRDD(calculate_distinct)

ssc.start()
ssc.awaitTermination(timeout=600)

print("Duration:", time.time()-start)