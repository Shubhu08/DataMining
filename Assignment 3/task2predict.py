#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 02:30:37 2021

@author: shubhashreedash
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 01:51:27 2021

@author: shubhashreedash
"""

import sys
import json
from pyspark import SparkContext
from math import sqrt
import time

def calculate_cosine(user_profile, business_profile):
    numerator = len(set(user_profile).intersection(set(business_profile)))
    denominator = sqrt(len(user_profile)) * sqrt(len(business_profile))
    return numerator/denominator if denominator != 0 else 0

if __name__ == '__main__':
    #input_file_path = sys.argv[1]
    #model_file_path = sys.argv[2]
    #output_file_path = sys.argv[3]
    

    input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/test_review.json"    
    output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task2.predict"
    model_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task2.model"
    #stop_word_file = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/stopwords"
    
    #global stop_words
    #stop_words = set(word.strip() for word in open(stop_word_file))
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    start_time = time.time()
        
    model_rdd = sc.textFile(model_file_path)\
        .map(lambda line : json.loads(line))\
            
#    print("Model rdd",model_rdd.take(2))
#        .collectAsMap()
        
#    user_profile = json.loads(model_rdd['user_profile'])
#    business_profile = json.loads(model_rdd['business_profile'])
    
    user_profile = model_rdd.filter(lambda item : item['type'] == "user_profile")\
        .map(lambda item : (item['profile_id'], item['profile_vector']))\
        .collectAsMap()
        
    business_profile = model_rdd.filter(lambda item : item['type'] == "business_profile")\
        .map(lambda item : (item['profile_id'], item['profile_vector']))\
        .collectAsMap()

    #print("User profile ",user_profile['D43OWyfzIQjL8feJpYh2SQ'])
    #print("Business profile ",business_profile['UT6L3b7Zll_nvRidijiDSA'])
    
    test_rdd = sc.textFile(input_file_path)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['user_id'], line['business_id'] ))

    
    result = test_rdd.map(lambda item : (item[0], item[1], calculate_cosine(user_profile.get(item[0], []), business_profile.get(item[1], []))))\
        .filter(lambda item : item[2] >= 0.01)\
        .collect()
        
    with open(output_file_path,"w+") as output_file:
        item_dict = {}
        for item_set in result:
            item_dict['user_id'] = item_set[0]
            item_dict['business_id'] = item_set[1]
            item_dict['sim'] = item_set[2]
            
            output_file.writelines(json.dumps(item_dict) + "\n")
            
        output_file.close()
        
    print("Duration :",time.time() - start_time)   
        