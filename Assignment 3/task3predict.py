#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 22:00:29 2021

@author: shubhashreedash
"""
import sys
from pyspark import SparkContext
import json
from statistics import mean
from math import sqrt

def calculate_average_with_business(business_id, user_business_map):
    sum_of_ratings = sum(user_business_map.values()) - user_business_map[business_id]
    number_of_business = len(user_business_map.values())-1
    return sum_of_ratings / number_of_business  if number_of_business >= 1 else 0

def calculate_average(user_business_map):
    sum_of_ratings = sum(user_business_map.values())
    number_of_business = len(user_business_map.values())
    return sum_of_ratings / number_of_business  if number_of_business >= 1 else 0

def make_user_based_prediction(user_id,user_ratings, other_users, pearson_values_map):
    user_average = calculate_average(user_ratings)
    numerator = 0
    denominator = 0
    
    for other_user_id, other_user_rating in other_users.items():
        pearson = pearson_values_map.get(tuple(sorted([user_id,other_user_id])), 0)
        numerator = numerator + (other_user_rating * pearson)
        denominator = denominator + abs(pearson)
        
    if numerator == 0 or denominator == 0 :
        final = 0
    else:            
        final = numerator /   denominator
            
    return (user_average + final) if (user_average + final) <= 5 else 5


def make_item_based_prediction(user_id, business_id, user_ratings, pearson_values_map):
    numerator = 0
    denominator = 0   
    pearson_values = []
    for other_business_id, other_business_rating in user_ratings.items():
        pearson_values = pearson_values + [(pearson_values_map.get(tuple(sorted([business_id,other_business_id])), 0), other_business_rating)]
    
    #print("Pearson Values : ",pearson_values)
    #print("Pearson Values Sorted : ",sorted(pearson_values)[-5:])
    for pearson, other_business_rating in sorted(pearson_values)[-6:]:
        numerator = numerator + (other_business_rating * pearson)
        denominator = denominator + abs(pearson)
        
    if numerator == 0 or denominator == 0 :
        final = calculate_average(user_ratings)
    else:            
        final = numerator /   denominator
        
    return final

def join_map(dict1, dict2):
    return {**dict1, **dict2}

def reduce_map(dict1):
    for key in dict1:
        if len(dict1[key]) > 1:
            dict1[key] = mean(dict1[key])
        else :
            dict1[key] = dict1[key][0]
    return dict1

if __name__ == "__main__":
    #input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/train_review.json"
    #output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task3user.predict"
    #testing_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/test_review.json"
    #model_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task3user.model"
    #case = "user_based"
    
    #input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/train_review.json"
    #output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task3item.predict"
    #testing_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/test_review.json"
    #model_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task3item.model"
    #case = "item_based"

    
    input_file_path = sys.argv[1]
    testing_file_path = sys.argv[2]
    model_file_path = sys.argv[3]
    output_file_path = sys.argv[4]    
    case = sys.argv[5]
    
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    input_rdd = sc.textFile(input_file_path)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['business_id'], line['user_id'], line['stars'] ))

    if case == "item_based":
            
        user_index = input_rdd.map(lambda item : item[1])\
                        .distinct()\
                        .sortBy(lambda item : item)\
                        .zipWithIndex()
                        #.map(lambda item : (item[0] : item[1]))
    
        user_index_map = user_index.collectAsMap()
        
        index_user_rdd = user_index.map(lambda item : (item[1], item[0]))    
        index_user_map = index_user_rdd.collectAsMap()
        
        business_index = input_rdd.map(lambda item : item[0])\
                        .distinct()\
                        .sortBy(lambda item : item)\
                        .zipWithIndex()                        
        business_index_map = business_index.collectAsMap()
        index_business_map = business_index\
                            .map(lambda item : (item[1], item[0]))\
                            .collectAsMap()
        
        user_business_rdd = input_rdd\
                            .map(lambda item : (user_index_map[item[1]], {business_index_map[item[0]] : [item[2]]})) \
                            .reduceByKey(lambda items, item : join_map(items,item))\
                            .map(lambda item : (item[0], reduce_map(item[1])))
        user_business_map = user_business_rdd.collectAsMap()   
        
        business_user_rdd = input_rdd.filter(lambda item : item[0] in business_index_map.keys())\
                            .map(lambda item : (business_index_map[item[0]], {user_index_map[item[1]]: [(item[2] - calculate_average_with_business(business_index_map[item[0]], user_business_map[user_index_map[item[1]]]))]})) \
                            .reduceByKey(lambda items, item : join_map(items,item))\
                            .map(lambda item : (item[0], reduce_map(item[1])))
        business_user_map = business_user_rdd.collectAsMap()
                
        testing_rdd = sc.textFile(testing_file_path)\
                        .map(lambda line : json.loads(line))\
                        .map(lambda line : (user_index_map.get(line['user_id'],-1), business_index_map.get(line['business_id'],-1)))\
                        .filter(lambda item: item[0] != -1 and item[1] != -1)
                                                
        model_map = sc.textFile(model_file_path)\
                        .map(lambda line : json.loads(line))\
                        .map(lambda line : ( tuple(sorted([business_index_map.get(line["b1"],-1), business_index_map.get(line["b2"], -1)])), line["sim"]))\
                        .filter(lambda item: item[0][0] != -1 and item[0][1] != -1)\
                        .collectAsMap()
        
        #print("Model ",len(model_map))
        
        final_predictions = testing_rdd.leftOuterJoin(user_business_rdd)\
                .map(lambda item : (index_user_map[item[0]] , index_business_map[item[1][0]] , make_item_based_prediction(item[0] , item[1][0] , item[1][1] , model_map)))\
                .collect()
                
        with open(output_file_path,"w+") as output_file:
            item_dict = {}
            for item_set in final_predictions:
                item_dict['user_id'] = item_set[0]
                item_dict['business_id'] = item_set[1]
                item_dict['stars'] = item_set[2]
                
                output_file.writelines(json.dumps(item_dict) + "\n")
                
            output_file.close()                

    if case == "user_based":
            
        user_index = input_rdd.map(lambda item : item[1])\
                        .distinct()\
                        .sortBy(lambda item : item)\
                        .zipWithIndex()
                        #.map(lambda item : (item[0] : item[1]))
    
        user_index_map = user_index.collectAsMap()
        
        index_user_rdd = user_index.map(lambda item : (item[1], item[0]))    
        index_user_map = index_user_rdd.collectAsMap()
        
        business_index = input_rdd.map(lambda item : item[0])\
                        .distinct()\
                        .sortBy(lambda item : item)\
                        .zipWithIndex()                        
        business_index_map = business_index.collectAsMap()
        index_business_map = business_index\
                            .map(lambda item : (item[1], item[0]))\
                            .collectAsMap()
        
        user_business_rdd = input_rdd.filter(lambda item : item[0] in business_index_map.keys())\
                            .map(lambda item : (user_index_map[item[1]], {business_index_map[item[0]] : [item[2]]})) \
                            .reduceByKey(lambda items, item : join_map(items,item))\
                            .map(lambda item : (item[0], reduce_map(item[1])))
        user_business_map = user_business_rdd.collectAsMap()    
        
        business_user_rdd = input_rdd.filter(lambda item : item[0] in business_index_map.keys())\
                            .map(lambda item : (business_index_map[item[0]], {user_index_map[item[1]]: [(item[2] - calculate_average_with_business(business_index_map[item[0]], user_business_map[user_index_map[item[1]]]))]})) \
                            .reduceByKey(lambda items, item : join_map(items,item))\
                            .map(lambda item : (item[0], reduce_map(item[1])))
        business_user_map = business_user_rdd.collectAsMap()
                
        testing_rdd = sc.textFile(testing_file_path)\
                        .map(lambda line : json.loads(line))\
                        .map(lambda line : (business_index_map.get(line['business_id'],-1), user_index_map.get(line['user_id'],-1)))\
                        .filter(lambda item: item[0] != -1 and item[1] != -1)
                                                
        model_map = sc.textFile(model_file_path)\
                        .map(lambda line : json.loads(line))\
                        .map(lambda line : ( tuple(sorted([user_index_map.get(line["u1"],-1), user_index_map.get(line["u2"], -1)])), line["sim"]))\
                        .filter(lambda item: item[0][0] != -1 and item[0][1] != -1)\
                        .collectAsMap()
        
        final_predictions = testing_rdd.leftOuterJoin(business_user_rdd)\
                            .map(lambda item : (index_business_map[item[0]], index_user_map[item[1][0]], make_user_based_prediction(item[1][0], user_business_map[item[1][0]], item[1][1], model_map)))\
                            .collect()
                            
                            
        with open(output_file_path,"w+") as output_file:
            item_dict = {}
            for item_set in final_predictions:
                item_dict['user_id'] = item_set[1]
                item_dict['business_id'] = item_set[0]
                item_dict['stars'] = item_set[2]
                
                output_file.writelines(json.dumps(item_dict) + "\n")
                
            output_file.close()

        