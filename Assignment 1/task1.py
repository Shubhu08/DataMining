#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 27 12:17:06 2021

@author: shubhashreedash
"""
import sys
import json
from pyspark import SparkContext


excluded_chars = set(["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"])
#stop_words = "and"

def clean_text(string):
    #print("String",string)
    string = string.lower()
    new_string = ' '.join([(''.join([char if char not in excluded_chars else ' ' for char in word])) for word in string.split()])
    new_string = [word for word in new_string.split() if word not in stop_words]
    #print("New String",new_string)
    return new_string

if __name__ == '__main__':
    input_json = sys.argv[1]
    output_file_path = sys.argv[2]
    stop_word_file = sys.argv[3]
    year = int(sys.argv[4])
    m = int(sys.argv[5])
    n = int(sys.argv[6])
    
    global stop_words
    stop_words = set(word.strip() for word in open(stop_word_file))
    
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')

    review_output = {}
    input_rdd = sc.textFile(input_json).map(lambda line : json.loads(line))
    
    number_of_reviews = input_rdd.map(lambda line : (line['review_id'],1)).count() 
    #print(number_of_reviews)
    review_output['A'] = number_of_reviews
    
    number_of_reviews_in_year = input_rdd.map(lambda line : (line['review_id'],int(line['date'].split('-')[0])))\
        .filter(lambda line : line[1] == year)\
        .count() 
    #print(number_of_reviews_in_year)
    review_output['B'] = number_of_reviews_in_year
    
    user_review = input_rdd.map(lambda line : (line['user_id'],1))
    
    number_of_users = user_review.distinct().count()
    #print(number_of_users)
    review_output['C'] = number_of_users
    
    top_users = user_review.reduceByKey(lambda v,v1 : v+v1)\
        .takeOrdered(m, key = lambda key_val : (-key_val[1],key_val[0]))
    #print(top_users)
    review_output['D'] = top_users
    
    text_review_words = input_rdd.flatMap(lambda line : clean_text(line['text']))\
        .map(lambda key_val : (key_val,1))\
        .reduceByKey(lambda val,val1 : val+val1)\
        .takeOrdered(n,key = lambda key_val : (-key_val[1],key_val[0]))
    text_review_words = [word for (word,count) in text_review_words]        
    #print(text_review_words)
    review_output['E'] = text_review_words
    
    print(review_output)
    
    with open(output_file_path, 'w+') as output_file:
        json.dump(review_output, output_file)
    output_file.close()
    
    
    
    