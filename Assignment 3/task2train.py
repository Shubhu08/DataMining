#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 02:27:38 2021

@author: shubhashreedash
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 19:51:44 2021

@author: shubhashreedash
"""

import sys
import json
from pyspark import SparkContext
from math import log2
import time

excluded_chars = set(["(", "[", ",", ".", "!", "?", ":", ";", "]", ")", "=", "&"])
#stop_words = "and"

def clean_text(string):
    #print("String",string)
    string = string.lower()
    new_string = ' '.join([(''.join([char if char not in excluded_chars else ' ' for char in word])) for word in string.split()])
    new_string = [word for word in new_string.split() if word not in stop_words]
    #print("New String",new_string)
    return new_string


def select_top_200(word_tf_idf_list, frequent_words):
    word_tf_idf_list = filter(lambda word_count : word_count[0] in frequent_words, word_tf_idf_list)
    sorted_word_tf_idf_list = sorted(word_tf_idf_list, key = lambda word : (-word[1],word[0]))[:200]
    sorted_word_tf_idf_list = [word for (word,count) in sorted_word_tf_idf_list]
    return sorted_word_tf_idf_list
                                     
if __name__ == '__main__':
    #input_file_path = sys.argv[1]
    #output_file_path = sys.argv[2]
    #stop_word_file = sys.argv[3]

    input_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/train_review.json"    
    output_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/result/task2.model"
    stop_word_file = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 3/data/stopwords"
    
    global stop_words
    stop_words = set(word.strip() for word in open(stop_word_file))
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    start_time = time.time()
    input_rdd = sc.textFile(input_file_path).map(lambda line : json.loads(line))
    
    N = input_rdd.map(lambda item : item['business_id']).distinct().count()
    #print("Number of documents : ",N)
    
    business_review_words = input_rdd.flatMap(lambda line : [((line['business_id'], word),1) for word in clean_text(line['text'])])\
        .reduceByKey(lambda val,val1 : val+val1)\
        .map(lambda item : (item[0][0] , (item[0][1], item[1])))  
        
    #text_review_words = [word for (word,count) in text_review_words]        
    #print("Business review words :",business_review_words.take(10)) # ('eTXYID00jGxq1vZpntBUFw', ('boulder', 40))
    
    number_of_words = business_review_words\
        .map(lambda item : (item[1][0], item[1][1]))\
        .reduceByKey(lambda item1,item2 : item1 + item2)
    #print("Total Words : ",number_of_words.take(10))
    
    total_number_of_words = number_of_words.reduce(lambda item1, item2 : ("count" , item1[1]+item2[1]))#.map(lambda item : item[1])
    #print("total number of words : ",total_number_of_words)
    
    rare_frequency_threshold = total_number_of_words[1] * 1e-4
    #print("frequency threshold : ",rare_frequency_threshold)
    
    frequent_words = number_of_words\
        .filter(lambda item : item[1] > rare_frequency_threshold)\
        .map(lambda item : item[0])\
        .collect()
#    print("Frequent words", frequent_words)
#    business_review_words =  business_review_words\
#        .filter(lambda item : item[1][0] not in non_frequent_words)\
#        .persist()
    
    max_frequency = business_review_words.map(lambda item : (item[0] , item[1][1]))\
                    .reduceByKey(lambda val1, val2 : max(val1,val2))
    
    #print(max_frequency.take(10))
    
    word_tf = business_review_words.leftOuterJoin(max_frequency)\
        .map(lambda item : (item[1][0][0], (item[0] ,item[1][0][1] / item[1][1])))
    #print("TF : ",word_tf.take(10))
    
    
    word_idf = business_review_words.map(lambda item : (item[1][0], 1))\
                                    .reduceByKey(lambda val,val1 : val+val1)\
                                    .map(lambda item: (item[0] , log2( N/item[1] )))
                                
    #print("IDF : ",word_idf.take(10))
    
    #('resulted', (('n8Zqqhff-2cxzWt_nwhU2Q', 0.002631578947368421), 5.369562169319433))
    word_tf_idf = word_tf.leftOuterJoin(word_idf)\
        .map(lambda item : (item[1][0][0] , (item[0] , item[1][0][1] * item[1][1])))\
        .groupByKey()
    
    business_profile = word_tf_idf\
        .map(lambda item : (item[0], select_top_200(list(item[1]), frequent_words)))
        
    business_profile_map = business_profile.collectAsMap()
    #business_profile = business_profile.collect()
        
        
    user_profile = input_rdd.map(lambda item : (item['user_id'], business_profile_map[item['business_id']]))\
        .reduceByKey(lambda items,item : items+item)\
        .map(lambda item : (item[0] , list(set(item[1]))))\
        .collect()
    
    #print("User profile : ",user_profile.take(2))
        
    #print("TF*IDF : ",word_tf_idf)
    print("Before Write Duration :",time.time() - start_time)
    
    with open(output_file_path,"w+") as output_file:
        item_dict = {}
    
        for business_id, profile in business_profile_map.items():
            item_dict['type'] = "business_profile"            
            item_dict['profile_id'] = business_id
            item_dict['profile_vector'] = profile
        
            output_file.writelines(json.dumps(item_dict) + "\n")
        
        for profile in user_profile:
            item_dict['type'] = "user_profile"            
            item_dict['profile_id'] = profile[0]
            item_dict['profile_vector'] = profile[1]
        
            output_file.writelines(json.dumps(item_dict) + "\n")
            
        output_file.close()
 
    
    print("Duration :",time.time() - start_time)
    
