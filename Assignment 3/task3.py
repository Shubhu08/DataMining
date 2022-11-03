#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 12 23:57:16 2021

@author: shubhashreedash
"""

import sys
from pyspark import SparkContext
import json
import random
from itertools import combinations
import time
from collections import ChainMap
from statistics import mean
from math import sqrt

THRESHOLD = 3

def create_appropriate_pairs(business_id, business_map):
    business_user = business_map[business_id]
    result = []
    
    for c_id in range(0,business_id):
        candidate_user = business_map[c_id]
        common_users = set(business_user.keys()).intersection(set(candidate_user.keys()))
        if len(common_users) >= THRESHOLD:
            numerator = 0
            denominator1 = 0 
            denominator2 = 0
            b1_avg = mean(business_user.values())
            b2_avg = mean(candidate_user.values())
            for user in common_users:
                b1_r = business_user[user] - b1_avg
                b2_r = candidate_user[user] - b2_avg
                numerator = numerator + (b1_r*b2_r)
                denominator1 = denominator1 + b1_r**2
                denominator2 = denominator2 + b2_r**2
                #print("Elements : ", b1_r, b1_r**2, b2_r, b2_r**2)
            
            if numerator == 0 or denominator1 == 0 or denominator2 == 0:
                pearson = 0
            
            else:            
                pearson = numerator / (sqrt(denominator1) * sqrt(denominator2))
                
            result.append((business_id,c_id,pearson))
    return result


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


def calculate_pearson(user1, user1_reviews, user2, user2_reviews):
    common_business = set(user1_reviews.keys()).intersection(set(user2_reviews.keys()))
    if len(common_business) >= THRESHOLD:
        numerator = 0
        denominator1 = 0 
        denominator2 = 0
        b1_avg = mean(user1_reviews.values())
        b2_avg = mean(user2_reviews.values())
        for user in common_business:
            b1_r = user1_reviews[user] - b1_avg
            b2_r = user2_reviews[user] - b2_avg
            numerator = numerator + (b1_r*b2_r)
            denominator1 = denominator1 + b1_r**2
            denominator2 = denominator2 + b2_r**2
            #print("Elements : ", b1_r, b1_r**2, b2_r, b2_r**2)
        
        if numerator == 0 or denominator1 == 0 or denominator2 == 0:
            pearson = 0
        
        else:            
            pearson = numerator / (sqrt(denominator1) * sqrt(denominator2))         
        
        return (user1,user2,pearson)
    


if __name__ == "__main__":
    input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/train_review.json"
    output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task3item.model"
    case = "item_based"
    #input_file_path = sys.argv[1]
    #output_file_path = sys.argv[2]
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    
    
    input_rdd = sc.textFile(input_file_path)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['business_id'], line['user_id'], line['stars'] ))
        
    #print("Input Map : ",input_rdd.collect())
    
    
    if case == "item_based" :
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
        
        business_index = input_rdd.map(lambda item : (item[0], 1))\
                        .reduceByKey(lambda vals,val1 : vals+val1)\
                        .filter(lambda item : item[1] >= 3)\
                        .distinct()\
                        .map(lambda item : item[0])\
                        .sortBy(lambda item : item)\
                        .zipWithIndex()
                        #.map(lambda item : {item[0] : item[1]})
                        
        business_index_map = business_index.collectAsMap()
        #print("Business Index Map : \n",business_index_map)
        
        index_business_map = business_index\
                            .map(lambda item : (item[1], item[0]))\
                            .collectAsMap()
        
        business_user_rdd = input_rdd.filter(lambda item : item[0] in business_index_map.keys())\
                            .map(lambda item : (business_index_map[item[0]], {user_index_map[item[1]]: item[2]})) \
                            .groupByKey()\
                            .map(lambda item : (item[0], dict(ChainMap(*item[1]))))
        business_user_map = business_user_rdd.collectAsMap()
        
        #print("Business User Map : ",business_user_map)
        
        final_pairs = business_user_rdd\
                            .flatMap(lambda item : create_appropriate_pairs(item[0], business_user_map))\
                            .map(lambda item : (index_business_map[item[0]], index_business_map[item[1]], item[2]))\
                            .collect()
                            
        #print("Cartesian Map : ",final_pairs)
        
        with open(output_file_path,"w+") as output_file:
            item_dict = {}
            for item_set in final_pairs:
                item_dict['b1'] = item_set[0]
                item_dict['b2'] = item_set[1]
                item_dict['sim'] = item_set[2]
                
                output_file.writelines(json.dumps(item_dict) + "\n")
                
            output_file.close()
        
        
    if case == "user_based":
            
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
        NUMBER_OF_HASH = 50
        BAND = 50
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
        candidate_pairs = set()
        for i in range(BAND-1):
            temp_candidate_pairs = signature_matrix.map(lambda item : (split_rows(item[1], BAND, ROW, i), item[0]))\
                            .groupByKey()\
                            .filter(lambda item : len(item[1])>1)\
                            .flatMap(lambda item : [tuple(sorted(pair)) for pair in combinations(list(item[1]),2)])
            
            #print("Temp Pairs : ",temp_candidate_pairs.collect())
            candidate_pairs = candidate_pairs.union(set(temp_candidate_pairs.collect()))            
        
        #print("Candidate Pairs : ",candidate_pairs)
        
        # find similarity 
        
        similar_pairs = sc.parallelize(candidate_pairs)\
                        .map(lambda item : (item[0], item[1], get_similarity(business_user_map[item[0]], business_user_map[item[1]])))\
                        .filter(lambda item : item[2] >= 0.01)\
                        
        # find pearson
        
        user_business_rdd = input_rdd.filter(lambda item : item[0] in business_index_map.keys())\
                            .map(lambda item : (user_index_map[item[1]], {business_index_map[item[0]] : item[2]})) \
                            .groupByKey()\
                            .map(lambda item : (item[0], dict(ChainMap(*item[1]))))
        user_business_map = user_business_rdd.collectAsMap()
        
        #print("User Business Map : ",business_user_map)
        
        ### Modify for similar pairs
        final_pairs = similar_pairs.map(lambda item : calculate_pearson(item[0], user_business_map[item[0]], item[1], user_business_map[item[1]]))\
                        .filter(lambda item : item != None)\
                        .map(lambda item : (index_user_map[item[0]], index_user_map[item[1]], item[2]))\
                        .collect()
        #candidate_pairs = user_business_rdd\
        #                    .flatMap(lambda item : create_appropriate_pairs(item[0], user_business_rdd))\

        #print("Cartesian Map : ",final_pairs)   
        
        with open(output_file_path,"w+") as output_file:
            item_dict = {}
            for item_set in final_pairs:
                item_dict['u1'] = item_set[0]
                item_dict['u2'] = item_set[1]
                item_dict['sim'] = item_set[2]
                
                output_file.writelines(json.dumps(item_dict) + "\n")
                
            output_file.close()
