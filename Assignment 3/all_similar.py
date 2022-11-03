#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 12 11:07:52 2021

@author: shubhashreedash
"""
import sys
from pyspark import SparkContext
import json
import random
from itertools import combinations
import time

def generate_hash_functions(number_of_hash):
    parameter_a_list = random.sample(range(1, sys.maxsize - 1), number_of_hash)
    parameter_b_list = random.sample(range(0, sys.maxsize - 1), number_of_hash)
    return zip(parameter_a_list,parameter_b_list)

def apply_hash(a, b, m, item):
    return ((a * item + b) % 2333333) % m

def apply_multiple_hash(ab_tuple_list, m, item):
    item_hashes = []
    for (a,b) in ab_tuple_list:
        item_hashes.append(apply_hash(a, b, m, item))
    return item_hashes

def find_min(item):
    final = []
    transpose = zip(*item)
    for row in transpose:
        final.append(min(row))
    return final

def split_rows(row_item, band, row, i):
    #chunk_list = []

    chunk = tuple(row_item[i*row:(i+1)*row])
    #chunk_list.append(chunk)
        
    #print("Chunk List : ",chunk_list)
    return chunk#_list

def get_similarity(set1, set2):
    return len(set1.intersection(set2))/len(set1.union(set2))


if __name__ == "__main__":
    input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/train_review.json"
    output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/all_similar.res"
    #input_file_path = sys.argv[1]
    #output_file_path = sys.argv[2]
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    input_rdd = sc.textFile(input_file_path)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['business_id'], line['user_id'] ))
        
    user_index = input_rdd.map(lambda item : item[1])\
                    .distinct()\
                    .sortBy(lambda item : item)\
                    .zipWithIndex()
                    #.map(lambda item : (item[0] : item[1]))

    user_index_map = user_index.collectAsMap()
    #print("User Index Map : \n",user_index_map)
    
    index_user_rdd = user_index.map(lambda item : (item[1], item[0]))

    index_user_map = index_user_rdd.collectAsMap()
    #print("Index User Map : \n",index_user_map)
    
    NUMBER_OF_USERS = index_user_rdd.count()
    NUMBER_OF_HASH = 25
    BAND = 25
    ROW = 1
    
    ab_list = list(generate_hash_functions(NUMBER_OF_HASH))
    #print("A-B list : ",ab_list)
    index_hash_map = index_user_rdd.map(lambda item : (item[0], apply_multiple_hash(ab_list, NUMBER_OF_USERS, item[0])))\
        .collectAsMap()
        
 #   print("Index User Hash Map : \n",index_hash_map)
    
    business_index = input_rdd.map(lambda item : item[0])\
                    .distinct()\
                    .sortBy(lambda item : item)\
                    .zipWithIndex()
                    #.map(lambda item : {item[0] : item[1]})
                    
    business_index_map = business_index.collectAsMap()
    #print("Business Index Map : \n",business_index_map)
    index_business_map = business_index\
                        .map(lambda item : (item[1], item[0]))\
                        .collectAsMap()
    
    business_user_rdd = input_rdd.map(lambda item : (business_index_map[item[0]], user_index_map[item[1]])) \
                        .groupByKey()\
                        .map(lambda item : (item[0], set(item[1])))
    business_user_map = business_user_rdd.collectAsMap()
    #print("Business User Map : ",business_user_map)
    
    signature_matrix = business_user_rdd.map(lambda key_val : (key_val[0], [index_hash_map[item] for item in key_val[1]]))\
                        .map(lambda item : (item[0], find_min(item[1])))\
#                        .map(lambda item : (item[0], ))
    
    #print("Signature Matrix : ",signature_matrix.collect())

    candidate_pairs = combinations(user_index_map, 2)
    
    # find similarity 
    
    final_pairs = sc.parallelize(candidate_pairs)\
                    .map(lambda item : (index_business_map[item[0]], index_business_map[item[1]], get_similarity(business_user_map[item[0]], business_user_map[item[1]])))\
                    .filter(lambda item : item[2] >= 0.01)\
                    .collect()
                    
    #print("Final Pairs : ",final_pairs)
    
    with open(output_file_path,"w+") as output_file:
        item_dict = {}
        for item_set in final_pairs:
            item_dict['b1'] = item_set[0]
            item_dict['b2'] = item_set[1]
            item_dict['sim'] = item_set[2]
            
            output_file.writelines(json.dumps(item_dict) + "\n")
            
        output_file.close()
    
    
    
    