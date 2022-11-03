#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  5 16:08:45 2021

@author: shubhashreedash
"""
from copy import deepcopy
from itertools import product, combinations
from pyspark import SparkContext, SparkConf
from math import ceil
import sys
import time

def count_in_basket(baskets, item):
    count = 0
    for basket in baskets:
        if set(item).issubset(set(basket)):
            count = count + 1

    return count

def has_frequent_subsets(item, frequent_items,k):
    item_subsets = combinations(item, k-1)
    count = 1
    #print("Item : ",item)
    #print("Frequent : ",frequent_items)
    for subset in item_subsets:
    #    print("Subset : ",subset)
        if subset in frequent_items:
            count = count * 1
        else:
            count = 0
    #print("Count : ",count == 1)
    return count == 1

def a_priori(baskets,threshold):
    #pass 1

    baskets = list(baskets)
    #print("Baskets : ",baskets)
    #print("Threshold : ",threshold)
    single_item_dict = {}
    
    for basket in baskets:
        for item in basket:
            single_item_dict[item] = single_item_dict.get(item,0) + 1
            #print("ITEM : ",single_item_dict[item])

    #print("Single Items ",single_item_dict)

    frequent_item_set = set(filter(lambda key_val : key_val[1] >= threshold,single_item_dict.items()))
    frequent_item_set = set(map(lambda item : (item[0],),frequent_item_set))
    #print(frequent_item_set)
    
    candidate_list = deepcopy(frequent_item_set)
    k=2
    while candidate_list != None and len(candidate_list) > 0:
        product_list = map(lambda tup : tuple(sorted(set(tup[0]).union(set(tup[1])))),list(product(candidate_list,candidate_list)))
        
        #print("Product List : ",list(deepcopy(product_list)))
        
        temp_candidate_list = set([])
        
        for product_tuple in product_list:
            
            if len(product_tuple) == k:
                #print("COUNT : ",count_in_basket(baskets, product_tuple))
                #print("Condition : ",has_frequent_subsets(product_tuple,frequent_item_set,k) and count_in_basket(baskets, product_tuple) >= threshold)
                if has_frequent_subsets(product_tuple,frequent_item_set,k) and count_in_basket(baskets, product_tuple) >= threshold:
                    temp_candidate_list = temp_candidate_list.union([product_tuple])
                 #   print("Temp List : ",temp_candidate_list)
        candidate_list = deepcopy(temp_candidate_list)
        
        frequent_item_set = frequent_item_set.union(set(temp_candidate_list))
        k = k + 1
        #print("Candidate List : ",candidate_list)
        #print("CANDIDATE : ",temp_candidate_list)
    #print("FREQUENT : ",list(frequent_item_set))
    
    return list(frequent_item_set)

def call_a_priori(basket,fullsize,threshold):
    basket = list(basket)
    size = len(basket)
    #print("Size ",size)
    return a_priori(basket,ceil(size/fullsize*threshold))
    

def SON(basket_rdd, threshold):
    #first map
    size_of_data = int(basket_rdd.count())
    #print("Size of Data : ",size_of_data)
    #print("Parition sizes : ",basket_rdd.glom().map(len).collect())
    #print("Float Threshold",float(threshold))
    baskets = basket_rdd.collect()
    
    #print("entire ",baskets)
    #candidates = basket_rdd.mapPartitions(lambda partition : [ceil(len(list(partition))/size_of_data*threshold)])
    candidates = basket_rdd.mapPartitions(lambda partition : call_a_priori(partition, size_of_data, threshold))\
                            .distinct()\
                            .sortBy(lambda candidate : (len(candidate),candidate))\
    #                        .map(lambda key_val : key_val[0])
    
    #print("Candidates : ",candidates.collect())
    final_candidates = candidates.collect()
    
    frequents = candidates.map(lambda item_set : (item_set,count_in_basket(baskets, item_set)))\
                            .reduceByKey(lambda vals,val1 : vals+val1)\
                            .filter(lambda item : item[1] >= threshold)\
                            .map(lambda item : item[0])\
                            .sortBy(lambda candidate : (len(candidate),candidate))
    
    #print("Frequents : ",frequents.collect())
    final_frequents = frequents.collect()
    return final_candidates,final_frequents

def write_output(output_file_path,candidates, frequents):
    #output_string = str(candidate).replace(",)", ")").replace(", ", ",")
    output_string = "Candidates:"
    k=0
    for item_set in candidates:
        if k<len(item_set):
            output_string = output_string + "\r\n"
            k = k+1            
        else:
            output_string = output_string + ","
        if len(item_set) == 1:
            temp_string = str(item_set).replace(",)", ")")
            output_string = output_string + temp_string
        else:
            output_string = output_string + str(item_set) #.replace(", ", ",")
    
    output_string = output_string + "\r\nFrequent Itemsets:"
    k=0
    for item_set in frequents:
        if k<len(item_set):
            output_string = output_string + "\r\n"
            k = k+1
        else:
            output_string = output_string + ","
        if len(item_set) == 1:
            temp_string = str(item_set).replace(",)", ")")
            output_string = output_string + temp_string
        else:
            output_string = output_string + str(item_set) #.replace(", ", ",")
            
    with open(output_file_path,"w+") as file:
        file.write(output_string)
    file.close()
    
if __name__ == "__main__":
    case = sys.argv[1]
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3] # "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 2/data/small1.csv" #
    output_file_path = sys.argv[4]
    
    #set_try = set([('1','2'),('2','3'),('1','3'),('2','4'),('1','2','3'),('2','3','4'),('2','4'),('1','2','4'),('1','2','5','4'),('1','2','3'),('1','2','3','4','5'),('1','2','6','4','5')])
    #a_priori(set_try,2)
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    #conf = SparkConf().setMaster("local").setAppName("My App")
    #sc = SparkContext(conf = conf)
    start_time = time.time()
    if case == "1":
        input_rdd = sc.textFile(input_file_path)
        header = input_rdd.first()
        input_rdd = input_rdd.filter(lambda line : line != header)\
            .map(lambda line: (line.split(',')[0], (line.split(',')[1],)))\
            .reduceByKey(lambda vals , val1 : val1 + vals)\
            .map(lambda item : item[1])
            
        #print(input_rdd.sortBy(lambda candidate : (len(candidate),candidate)).collect())
        candidate , frequents = SON(input_rdd, support_threshold)

    else:
        input_rdd = sc.textFile(input_file_path)
        header = input_rdd.first()
        input_rdd = input_rdd.filter(lambda line : line != header)\
            .map(lambda line: (line.split(',')[1], (line.split(',')[0],)))\
            .reduceByKey(lambda vals , val1 : val1 + vals)\
            .map(lambda item : item[1])
            
        #print(input_rdd.sortBy(lambda item : (item[0])).collect())
#        print(input_rdd.sortBy(lambda candidate : (len(candidate),candidate)).count())
        candidate , frequents = SON(input_rdd, support_threshold)
        
    write_output(output_file_path,candidate, frequents)
    
    print("Duration: ",time.time()-start_time)
    #print("A - Priori")
    #a_pri_list = a_priori(input_rdd.collect(),support_threshold)
    #a_pri_list.sort(key = lambda item : (len(item),str(item)) )
    
    #print(a_pri_list)
    """   
    from pyspark.mllib.fpm import FPGrowth
    
    input_rdd = sc.textFile(input_file_path)
    header = input_rdd.first()
    input_rdd = input_rdd.filter(lambda line : line != header)\
        .map(lambda line: (line.split(',')[1], [line.split(',')[0]]))\
        .reduceByKey(lambda vals , val1 : val1 + vals)\
        .map(lambda item : item[1])
        
    #print(input_rdd.collect())
       
    model = FPGrowth.train(input_rdd, minSupport=0.36)
    result = model.freqItemsets().sortBy(lambda candidate : (len(candidate),candidate)).collect()
    #for fi in result:
    #    print(fi)
    """