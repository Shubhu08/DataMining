#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  7 21:15:27 2021

@author: shubhashreedash
"""

from copy import deepcopy
from itertools import product, combinations
from pyspark import SparkContext, StorageLevel
from math import ceil
import sys
import time

def count_in_basket(baskets, item, threshold):
    count = 0
    for basket in baskets:
        if set(item).issubset(set(basket)):
            count = count + 1
            if count >= threshold:
                return count
    return count

def clean_basket(basket, frequent):
    return tuple(sorted(set(basket).intersection(set(frequent))))

def count_in_candidates(item, candidates):
    count_list = []
    for candidate in candidates:
        if set(candidate).issubset(set(item)):
            count_list = count_list + [(candidate,1)]
    return count_list

def has_frequent_subsets(item, frequent_items,k):
    item_subsets = combinations(item, k-1)
    count = False
    #print("Item : ",item)
    #print("Frequent : ",frequent_items)
    for subset in item_subsets:
    #    print("Subset : ",subset)
        if tuple(sorted(subset)) in frequent_items:
            count = True
        else:
            return False
    #print("Count : ",count == 1)
    return True

def a_priori(baskets,threshold):
    #pass 1

    baskets = list(baskets)
    #print("Baskets : ",baskets)
    #print("Threshold : ",threshold)
    single_item_dict = {}
    frequent_item_set = set()
    for basket in baskets:
        for item in basket:
            single_item_dict[item] = single_item_dict.get(item,0) + 1
            if single_item_dict.get(item,0) != -1 and single_item_dict[item] >= threshold:
                frequent_item_set.add(item)
                single_item_dict[item] = -1
    #print("Single Items ",single_item_dict)

#    frequent_item_set = set(filter(lambda key_val : key_val[1] >= threshold,single_item_dict.items()))
#    frequent_item_set = set(map(lambda item : (item[0],),frequent_item_set))
    #print(frequent_item_set)

    #candidate_list = deepcopy(frequent_item_set)
    #pass 2
    
    candidate_count_dict = {}
    candidate_list = set([])
    for basket in baskets:
        cleaned_basket = clean_basket(basket, frequent_item_set)
        #print("cleaned basket : ",cleaned_basket)
        candidates_in_basket = set(map(lambda subset : tuple(sorted(subset)), list(combinations(cleaned_basket, 2))))
        #print("Candidates in basket : ",candidates_in_basket)
        for candidate in candidates_in_basket:
            if candidate_count_dict.get(candidate,0) != -1:
                candidate_count_dict[candidate] = candidate_count_dict.get(candidate,0) + 1
                if candidate_count_dict[candidate] >= threshold:
                    candidate_count_dict[candidate] = -1
                    candidate_list = candidate_list.union([candidate])

    #print("Candidate list ",candidate_list)
    frequent_item_set = set(map(lambda item : (item,),frequent_item_set))
    frequent_item_set = frequent_item_set.union(set(candidate_list))
    
    k=3
    while candidate_list != None and len(candidate_list) > 0:
        product_list = set(map(lambda tup : tuple(sorted(set(tup[0]).union(set(tup[1])))),list(combinations(candidate_list,2))))
        #print("Product List : ",list(deepcopy(product_list)))
        temp_candidate_list = set([])

        for product_tuple in product_list:

            if len(product_tuple) == k :
                #print("COUNT : ",count_in_basket(baskets, product_tuple))
                #print("gsgsgCondition : ",has_frequent_subsets(product_tuple,frequent_item_set,k) and count_in_basket(baskets, product_tuple) >= threshold)
                if has_frequent_subsets(product_tuple,frequent_item_set,k):
                    temp_candidate_list = temp_candidate_list.union([product_tuple])
                 #   print("Temp List : ",temp_candidate_list)

        candidate_list = set()
        candidate_count_dict = {}
        for basket in baskets:
            for candidate in temp_candidate_list:
                if candidate_count_dict.get(candidate,0) != -1 and set(candidate).issubset(basket):
                    candidate_count_dict[candidate] = candidate_count_dict.get(candidate,0) + 1
                    if candidate_count_dict[candidate] >= threshold:
                        candidate_count_dict[candidate] = -1
                        candidate_list = candidate_list.union([candidate])

        #candidate_list = deepcopy(temp_candidate_list)

        frequent_item_set = frequent_item_set.union(set(candidate_list))
        k = k + 1
        #print("Candidate List : ",candidate_list)
        #print("CANDIDATE : ",temp_candidate_list)
    #print("FREQUENT : ",list(frequent_item_set))

    return list(frequent_item_set)

def call_a_priori(basket,fullsize,threshold):
    #print("Call A priori")
    basket = list(basket)
    size = len(basket)
    return a_priori(basket,ceil(size/fullsize*threshold))


def SON(basket_rdd, threshold):
    #first map
    size_of_data = int(basket_rdd.count())
    #print("Size of Data : ",size_of_data)
    #print("Parition sizes : ",basket_rdd.glom().map(len).collect())
    #print("Float Threshold",float(threshold))
    #baskets = basket_rdd.collect()


    #candidates = basket_rdd.mapPartitions(lambda partition : [ceil(len(list(partition))/size_of_data*threshold)])
    candidates = basket_rdd.mapPartitions(lambda partition : call_a_priori(partition, size_of_data, threshold))\
                            .distinct()\
                            .sortBy(lambda candidate : (len(candidate),candidate))\
    #                        .map(lambda key_val : key_val[0])

    #print("Candidates calculated" , time.time())
    #print("Candidates : ",candidates.collect())
    final_candidates = candidates.collect()

    frequents = basket_rdd.flatMap(lambda item_set : (count_in_candidates(item_set,final_candidates)))\
                            .reduceByKey(lambda vals,val1 : vals+val1)\
                            .filter(lambda item : item[1] >= threshold)\
                            .map(lambda item : item[0])\
                            .sortBy(lambda candidate : (len(candidate),candidate))

    #print("Frequents : ",frequents.collect())
    final_frequents = frequents.collect()
    #print("Frequents calculated  ",time.time())
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
    filter_threshold = int(sys.argv[1])
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3] # "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 2/data/small1.csv" #
    output_file_path = sys.argv[4]

    #set_try = set([('1','2'),('2','3'),('1','3'),('2','4'),('1','2','3'),('2','3','4'),('2','4'),('1','2','4'),('1','2','5','4'),('1','2','3'),('1','2','3','4','5'),('1','2','6','4','5')])
    #a_priori(set_try,2)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')

    start_time = time.time()

    input_rdd = sc.textFile(input_file_path)
    header = input_rdd.first()
    #print("Header : ",header)
    input_rdd = input_rdd.filter(lambda line : line != header)\
        .map(lambda line: (line.split(',')[0], (line.split(',')[1],)))\
        .reduceByKey(lambda vals , val1 : val1 + vals)\
        .filter(lambda item : len(item[1]) > filter_threshold)\
        .map(lambda item : item[1])


    #print(input_rdd.count())
  #  input_rdd = input_rdd.coalesce(5)
    input_rdd.persist(StorageLevel.DISK_ONLY) #StorageLevel.DISK_ONLY
    #print("Persisted", time.time())
   # print(input_rdd.getNumPartitions())

    candidate , frequents = SON(input_rdd, support_threshold)
    print(len(frequents))
    write_output(output_file_path,candidate, frequents)

    print("Duration: ",time.time()-start_time)
    #print("A - Priori")
    #a_priori(input_rdd.collect(),support_threshold)
