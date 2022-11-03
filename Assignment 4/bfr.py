#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 21 19:03:19 2021

@author: shubhashreedash
"""

#from collections import namedtuple
#ClusterInfo = ("N", "SUM", "SUMSQ")
import random
from math import sqrt
from pyspark import SparkContext
import time
import os
from copy import deepcopy
import csv
import json
import sys

GLOBAL_ALPHA = 3

def compute_eucledian(point1, point2):
    return float(sqrt(sum([(a - b) ** 2 for (a, b) in zip(point1, point2)])))

def compute_mahalanobis(point1, point2, std):
    return float(sqrt(sum([((a - b) / sd) ** 2 for (a, b, sd) in zip(point1, point2, std)])))

## N:0, SUM: 1 SUMSQ:2
def get_new_cluster_info(cluster_values, data_set):
    result_dict = {}
    #print("Cluster values :",cluster_values)
    #print("Data set :",len(data_set), data_set)
    for key, point_values in cluster_values.items():
        new_list = [data_set[point] for point in point_values]
        N = len(point_values)
        SUM = [sum(i) for i in zip(*new_list)]
        SUMSQ = [sum([v ** 2 for v in i]) for i in zip(*new_list)]        
        result_dict[key] = (N,SUM,SUMSQ)
    return result_dict


# data_set => (index, tuple(points))
def update_centroid(cluster_values, data_set):
    result_dict = {}
    #print("Cluster Values : ",cluster_values)
    for key, point_values in cluster_values.items():
        new_list = [data_set[point] for point in point_values]
        N = len(point_values)
        centroid = [sum(i) / N for i in zip(*new_list)]  
        result_dict[key] = centroid
    return result_dict


def get_mean(N,SUM):
    return [ (i / N) for i in SUM]

def get_standard_deviation(N, mean, SUMSQ):
    mean_sq = [i**2 for i in mean]
    #print(list(zip(SUMSQ, mean_sq)))
    return [sqrt((a/N)-b) for a,b in zip(SUMSQ, mean_sq)]

def k_means(data_set, n_cluster, iterations):
    random.seed(200)
    centroid_info = {}
    data_set_keys = data_set.keys()
    random_sample_size = n_cluster if n_cluster < len(data_set_keys) else len(data_set_keys)
    for index, data_key in enumerate(random.sample(data_set_keys, random_sample_size)):
        #print("Index : ",index, "Data Key : ",data_key)
        centroid_info[index] = data_set[data_key]
    #print("Centroid Info : ",centroid_info)
        
    for _ in range(iterations):
        cluster_values = {}
        for key in data_set.keys():
            min_distance = float('inf')
            min_cluster_key = None
            for centroid in centroid_info.keys():
                #print("Centroid : ",centroid,"Key : ",key)
                cluster_distance = compute_eucledian(centroid_info[centroid],data_set[key])
                if cluster_distance < min_distance:
                    min_distance = cluster_distance
                    min_cluster_key = centroid
                    
            assigned_info = (min_cluster_key,min_distance)
            #print("Assigned info : ",assigned_info)
            cluster_values[assigned_info[0]] = cluster_values.get(assigned_info[0], []) + [key]        
        centroid_info = update_centroid(cluster_values, data_set)
        
    return cluster_values  ## cluster_set and cluster_map
    

def assign_to_cluster(point, clusters): #cluster set (N, SUM, SUMSQ) # point : (index,[dimensions])
    dimension = len(point[1]) - 1
    min_cluster_distance = float('inf')
    min_cluster_key = None
    for key, cluster in clusters.items():
        #print("Cluser : ", key)
        centroid = get_mean(cluster[0],cluster[1])
        std = get_standard_deviation(cluster[0],centroid, cluster[2])
        cluster_distance = compute_mahalanobis(point[1], centroid, std)
        if cluster_distance < GLOBAL_ALPHA * sqrt(dimension) and cluster_distance < min_cluster_distance:
            min_cluster_distance = cluster_distance
            min_cluster_key = key

    if min_cluster_key is not None:
        #print("Min cluster : ", min_cluster_distance, min_cluster_key, point[0])
        return tuple((min_cluster_key, point[0]))
    else:
        return tuple((-1, point))
    
def merge_clusters(cluster_set, cluster_map): #set defines N, SUM, SUMSQ | map defines cluster_index : point_index list
    result_cluster_set = {}
    result_cluster_map = {}
    cluster_keys = list(cluster_set.keys())
    remaining_clusters = set(cluster_set.keys())
    #print("Cluster keys",remaining_clusters)
    if cluster_keys == [] or cluster_keys == None:
        return cluster_set, cluster_map
    
    for index, cluster1 in enumerate(cluster_keys):
        if cluster1 in remaining_clusters:
            dimension = len(cluster_set[cluster1][1]) - 1
            centroid1 = get_mean(cluster_set[cluster1][0],cluster_set[cluster1][1])
            std1 = get_standard_deviation(cluster_set[cluster1][0],centroid1, cluster_set[cluster1][2])

            assigned_info = assigned_info = (None, float('inf'))
            for cluster2 in cluster_keys[index+1:]:
                if cluster2 in remaining_clusters:
                    centroid2 = get_mean(cluster_set[cluster2][0],cluster_set[cluster2][1])
                    #std2 = get_standard_deviation(cluster_set[cluster2][0],centroid2, cluster_set[cluster2][2])
                    cluster_distance = compute_mahalanobis(centroid2, centroid1, std1)
                    if cluster_distance < assigned_info[1] and assigned_info[1] < GLOBAL_ALPHA * sqrt(dimension):
                        assigned_info = (cluster2,cluster_distance)
            #print("Assigned info :", sorted(cluster_distance.items()), len(cluster_distance) != 0)
        
#            if(len(cluster_distance) != 0):
#                assigned_info = list(sorted(cluster_distance.items(), key=lambda kv: kv[1]))[0]

            
            if assigned_info != (None, float('inf')):
                #merge cluster1 and cluster 2
                cluster2 = assigned_info[0]
                new_SUM = [sum(i) for i in zip(cluster_set[cluster1][1],cluster_set[cluster2][1])]
                new_SUMSQ = [sum(i) for i in zip(cluster_set[cluster1][2],cluster_set[cluster2][2])]
                new_N = cluster_set[cluster1][0] + cluster_set[cluster2][0]
                
                result_cluster_set[index] = (new_N, new_SUM, new_SUMSQ)
                
                result_cluster_map[index] = list(set(cluster_map[cluster1] + cluster_map[cluster2]))
                remaining_clusters.remove(cluster1)
                remaining_clusters.remove(cluster2)
                
            else:
                result_cluster_set[index] = (cluster_set[cluster1][0], cluster_set[cluster1][1], cluster_set[cluster1][2])          
                result_cluster_map[index] = cluster_map[cluster1]
                remaining_clusters.remove(cluster1)
    
    #print("CS Cluster Map :",result_cluster_map)
    return result_cluster_set, result_cluster_map


def merge_cs_clusters_to_ds(cluster_set1, cluster_set2, cluster_map1, cluster_map2): #set defines N, SUM, SUMSQ | map defines cluster_index : point_index list merge compression set into discarded set
    result_cluster_set = cluster_set1
    result_cluster_map = cluster_map1
    cluster_keys = list(cluster_set2.keys())
    dimension = len(cluster_set1[0][1]) - 1
    result_rest_data_points = []
    #remaining_clusters = set(cluster_set2.keys())
    for index, cluster2 in enumerate(cluster_keys):
         centroid2 = get_mean(cluster_set2[cluster2][0],cluster_set2[cluster2][1])
         #std2 = get_standard_deviation(cluster_set2[cluster2][0],centroid1, cluster_set2[cluster2][2])
         cluster_distance = {}
         assigned_info = (None, float('inf'))
         for cluster1 in cluster_set1.keys():
                 centroid1 = get_mean(cluster_set1[cluster1][0],cluster_set1[cluster1][1])
                 std1 = get_standard_deviation(cluster_set1[cluster1][0],centroid2, cluster_set1[cluster1][2])
                 cluster_distance = compute_mahalanobis(centroid2, centroid1, std1)
                 if cluster_distance < GLOBAL_ALPHA * sqrt(dimension) and cluster_distance < assigned_info[1]:
                     assigned_info = (cluster1,cluster_distance)
         
        # print("Assigned info :", sorted(cluster_distance.items()), len(cluster_distance) != 0)
     
         if assigned_info != (None, float('inf')):
             #merge cluster1 and cluster 2
             cluster1 = assigned_info[0]
             new_SUM = [sum(i) for i in zip(result_cluster_set[cluster1][1],cluster_set2[cluster2][1])]
             new_SUMSQ = [sum(i) for i in zip(result_cluster_set[cluster1][2],cluster_set2[cluster2][2])]
             new_N = result_cluster_set[cluster1][0] + cluster_set2[cluster2][0]
             
             result_cluster_set[cluster1] = (new_N, new_SUM, new_SUMSQ)                
             result_cluster_map[cluster1] = list(set(result_cluster_map[cluster1] + cluster_map2[cluster2]))
             
         else:
             result_rest_data_points = result_rest_data_points + list(cluster_map2[cluster2])
            
    return result_cluster_set, result_cluster_map, result_rest_data_points


def merge_cluster_values(cluster_values1, cluster_values2):
    max_index = max(cluster_values1.keys()) if len(cluster_values1) >0 else -1
    index = max_index+1
    for key,values in cluster_values2.items():
        cluster_values1[index] = values + cluster_values1.get(index, [])    
    return cluster_values1

def update_cluster_set(old_discard_sets, new_cluster_values, data_set):
    result_cluster_set = {}
    new_discard_sets = get_new_cluster_info(new_cluster_values, data_set)
    for key in old_discard_sets.keys():
        old_discard_set = old_discard_sets[key]
        if key in new_discard_sets.keys():        
            new_SUM = [sum(i) for i in zip(old_discard_set[1],new_discard_sets[key][1])]
            new_SUMSQ = [sum(i) for i in zip(old_discard_set[2],new_discard_sets[key][2])]
            new_N = old_discard_set[0] + new_discard_sets.get(key)[0]
            result_cluster_set[key] = (new_N, new_SUM, new_SUMSQ)
        else: result_cluster_set[key] = old_discard_set

    return result_cluster_set

def separate_cs(cluster_values, min_len = 1):
    new_cluster_values = {}
    remaining = []
    index = 0
    for key, value in cluster_values.items():
        if len(value) > min_len:
            new_cluster_values[index] = value
            index = index + 1
        else:
            remaining = remaining + value
    return new_cluster_values, remaining

def merge_dict(kb1, kb2):
    merged_kb = {**kb1, **kb2}
    for key in merged_kb:
        value = merged_kb[key]
        if key in kb1 and key in kb2:
            merged_kb[key] = list(set(value).union(kb1[key]))
    return merged_kb

if __name__ == '__main__':
    start = time.time()
    # define input variables
    input_csv_dir = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 4/data/test2/"
    num_of_cluster = 10
    output_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 4/result/cluster2.json"
    intermediate_output_path = "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 4/result/intermediate2.csv"

    #os.environ['PYSPARK_PYTHON'] = 'usr/local/bin/python3.6'
    #os.environ['PYSPARK_DRIVER_PYTHON'] = 'usr/local/bin/python3.6'

    #input_csv_dir = sys.argv[1]
    #num_of_cluster = int(sys.argv[2])
    #output_path = sys.argv[3]
    #intermediate_output_path = sys.argv[4]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    file_paths = os.listdir(input_csv_dir)
    #print(file_paths)
    #file_paths.remove(".DS_Store")
    #print(file_paths)
    #print(len(os.listdir(input_csv_dir)))
    ds_cluster_set = {}
    cs_cluster_set = {}
    ds_cluster_values = {}
    cs_cluster_values = {}
    rs_points = []
    data_set = {}
    
    start_time = time.time()
    
    intermediate_result = [['round_id', 'nof_cluster_discard', 'nof_point_discard',
                              'nof_cluster_compression', 'nof_point_compression',
                              'nof_point_retained']]

    for index, file_path in enumerate(sorted(file_paths)):
        data_file_path = ''.join(input_csv_dir + "/" + file_path)
        
        input_rdd = sc.textFile(data_file_path).map(lambda row: row.split(",")) \
            .map(lambda item: (int(item[0]), list(map(eval, item[1:]))))

        print("File Number :",index)
        if index == 0:
            #total_length = input_rdd.count()
            #sample_size = 10000 if total_length > 10000 else int(total_length * 0.1)

            #sample_data = input_rdd.filter(lambda kv: kv[0] < sample_size).collectAsMap()
            sample_data = input_rdd.collectAsMap()
            
            #print("Data set key :",sample_data.keys())
            temp_cluster_values = k_means(sample_data, num_of_cluster * 5, 3)
            #print("DS Cluster :",ds_cluster_values)
            inlier_data , outlier_data = separate_cs(temp_cluster_values, min_len = 3)
            #print("Inlier points :",inlier_data)
            inlier_data = sc.parallelize(inlier_data.items())\
                .flatMap(lambda item : item[1])\
                .map(lambda item : (item, sample_data[item]))\
                .collectAsMap()

            #print("Inlier rdd : ",inlier_data.take(3)) 
           
            ds_cluster_values = k_means(inlier_data, num_of_cluster, 3)
            
            ds_cluster_set = get_new_cluster_info(ds_cluster_values, sample_data)
            #print("DS Cluster Set :",ds_cluster_set)
            
            rest_data = sc.parallelize(outlier_data)\
                .map(lambda item : (item, sample_data[item]))\
                .collectAsMap()
            
            #print("Rest Data :",len(rest_data), rest_data)
            cluster_values = k_means(rest_data, num_of_cluster * 3 , 3)

            data_set = deepcopy(rest_data)
            cs_cluster_values, rs_points = separate_cs(cluster_values, min_len = 3)
            #print("CS Clusters : ", cs_cluster_values)
            #print("RS : ",rs_points)
            
            cs_cluster_set = get_new_cluster_info(cs_cluster_values, rest_data)
            #print("CS Cluster Set :",cs_cluster_set)
            
            #final_result = #map the points using parallelize and flatmap
#            nof_cluster_discard = len(ds_cluster_values)
#            nof_point_discard = sum(map(lambda item : item[0], ds_cluster_set))
#            nof_cluster_compression = len(cs_cluster_values)
#            nof_point_compression = sum(map(lambda item : item[0], cs_cluster_set))
#            nof_point_retained = len(rs_points)
#            intermediate_result = intermediate_result + [index,nof_cluster_discard, nof_point_discard,nof_cluster_compression, nof_point_compression, nof_point_retained]

        else:
            
            total_new_data_set = {**data_set , **input_rdd.collectAsMap()}
            #print("Data Set Len :",len(total_new_data_set))
            
            #print("DS Cluster Set ",ds_cluster_set)
            step1_rdd = input_rdd\
                .map(lambda point: assign_to_cluster(point, ds_cluster_set))
                
            #print("Step 1 : ",step1_rdd.take(3))

            temp_ds_cluster_values = step1_rdd.filter(lambda item: item[0] != -1)\
                .groupByKey()\
                .mapValues(list)\
                .collectAsMap()

            #temp_ds_cluster_set = get_new_cluster_info(temp_ds_cluster_values, data_set)
            ds_cluster_set = update_cluster_set(ds_cluster_set, temp_ds_cluster_values, total_new_data_set)
            #print("DS Cluster Len :",len(ds_cluster_values))
            ds_cluster_values = merge_dict(ds_cluster_values,temp_ds_cluster_values)
            
            step2_int_rdd = step1_rdd.filter(lambda item: item[0] == -1) \
                .map(lambda point: point[1])
                
            #print("Interiem Data set :",step2_int_rdd.take(3))
            
            temp_cs_data_set = step2_int_rdd.collectAsMap()
            
            data_set = {**data_set , **temp_cs_data_set} # update dataset to add the aditional compresion set data points
            #print("Rest Data Set Len :",len(data_set))
            
            step2_rdd = step2_int_rdd\
                .map(lambda point: assign_to_cluster(point, cs_cluster_set))

            #print("Step 2 : ",step2_rdd.take(3))
            
            temp_cs_cluster_values = step2_rdd.filter(lambda item: item[0] != -1)\
                .groupByKey()\
                .mapValues(list)\
                .collectAsMap()
            
            #print("Temp cs cluster values :",temp_cs_cluster_values)
            
            #data_set = deepcopy(temp_cs_cluster_values)

            #temp_cs_cluster_set = get_new_cluster_info(temp_cs_cluster_values, data_set)
            cs_cluster_set = update_cluster_set(cs_cluster_set, temp_cs_cluster_values, data_set)
            cs_cluster_values = merge_cluster_values(cs_cluster_values, temp_cs_cluster_values)
            
            filtered_remaining_data_points = step2_rdd.filter(lambda item: item[0] == -1) 
            
            temp_rest_data = filtered_remaining_data_points \
                .map(lambda point: point[1]) \
                .collectAsMap()                
            
            print("Temp rest :",len(temp_rest_data))
            rest_data = {**rest_data, **temp_rest_data}
            print("Rest :",len(rest_data))

            #remaining_data_points = rest_data.keys()
            
            #rs_points = rs_points + list(remaining_data_points)

            temp_cluster_values = k_means(rest_data, num_of_cluster * 3, 3)

            temp_cs_cluster_values, temp_rs_points = separate_cs(temp_cluster_values)
            print("RS Points :",temp_rs_points)
            print("Temp Clusters :",temp_cs_cluster_values)
            rs_points = list(set(rs_points + list(temp_rs_points)))
            print("Rest points :",len(rs_points))
            
            cs_cluster_set = update_cluster_set(cs_cluster_set, temp_cs_cluster_values, data_set)
            cs_cluster_values = merge_cluster_values(cs_cluster_values, temp_cs_cluster_values)

            cs_cluster_set, cs_cluster_values = merge_clusters(cs_cluster_set, cs_cluster_values)
            
        #print("DS Cluster Set :",ds_cluster_set) 
        print("RS : ",len(rs_points))
        nof_cluster_discard = len(ds_cluster_values)
        nof_point_discard = sum(map(lambda item : item[1][0], ds_cluster_set.items()))
        nof_cluster_compression = len(cs_cluster_values)
        nof_point_compression = sum(map(lambda item : item[1][0], cs_cluster_set.items()))
        nof_point_retained = len(rs_points)
        intermediate_result = intermediate_result + [[index,nof_cluster_discard, nof_point_discard,nof_cluster_compression, nof_point_compression, nof_point_retained]]
        #print("Pass : ",time.time()- start_time)

        if index + 1 == len(file_paths):
            #print("Final DS Cluster : ",ds_cluster_values)
            #print("RS Points : ",rs_points)
            final_result_set , final_result_values , temp_rest_data = merge_cs_clusters_to_ds(ds_cluster_set, cs_cluster_set, ds_cluster_values, cs_cluster_values)
            
            
            #rs_points = map(lambda item : item[0], rs_points)
            final_result_values = {**final_result_values, **{-1:list(set(rs_points + temp_rest_data))}}
            
            final_result_map = sc.parallelize(final_result_values.items())\
                .flatMap(lambda item : [(str(item_val), item[0]) for item_val in item[1]])\
                .collectAsMap()
            #print("Final Result : ",final_result_map)
            print("Intermediate Result : ", intermediate_result)
            
            with open(intermediate_output_path, "w+") as output_file:
                writer = csv.writer(output_file)
                for value in intermediate_result:
                    writer.writerow(value)
                output_file.close()
            
            with open(output_path, "w+") as output_file:
                output_file.writelines(json.dumps(final_result_map))
                output_file.close()
            
    print("Duration :",time.time()-start_time)
        #intermediate_records.save_check_point(index + 1, discard_set, compression_set, retained_set)
