#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 26 19:52:21 2021

@author: shubhashreedash
"""

from sklearn.metrics.cluster import normalized_mutual_info_score
from pyspark import SparkContext
import json

sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")
sc.setLogLevel("ERROR")

clustering_file_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 4/data/cluster2.json"
label_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 4/result/cluster2.json"

ground_truth = sc.textFile(label_path).map(lambda line: json.loads(line)). \
    flatMap(lambda line: [(index, label) for index, label in line.items()]). \
    map(lambda pair: (int(pair[0]), pair[1])). \
    collect()
ground_truth.sort()
ground_truth = [cid for _, cid in ground_truth]

ground_truth_cluster_size = sc.textFile(label_path).map(lambda line: json.loads(line)). \
    flatMap(lambda line: [(index, label) for index, label in line.items()]). \
    map(lambda pair: (pair[1], pair[0])). \
    groupByKey(). \
    mapValues(len).collect()
ground_truth_cluster_size.sort(key=lambda pair: pair[1])

prediction_cluster_size = sc.textFile(clustering_file_path).map(lambda line: json.loads(line)). \
    flatMap(lambda line: [(index, label) for index, label in line.items()]). \
    map(lambda pair: (pair[1], pair[0])). \
    groupByKey(). \
    mapValues(len).collect()
prediction_cluster_size.sort(key=lambda pair: pair[1])
prediction = sc.textFile(clustering_file_path).map(lambda line: json.loads(line)). \
    flatMap(lambda line: [(index, label) for index, label in line.items()]). \
    map(lambda pair: (int(pair[0]), pair[1])). \
    collect()
prediction.sort()
prediction = [cid for _, cid in prediction]

print(normalized_mutual_info_score(ground_truth, prediction))
print(ground_truth_cluster_size)
print(prediction_cluster_size)