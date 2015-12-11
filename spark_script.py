from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import SVMWithSGD, SVMModel
import operator
import StringIO
import csv
import sys
import time

def parsePoint(line):  #Parsing the RDD to convert it into a LabeledPoint RDD
	values = [float(x) for x in line.split(',')]
	return LabeledPoint(values[0], values[1:])

def gen_row(row):  #Unpacking the tuples to lists
    s = StringIO.StringIO()
    w = csv.writer(s)
    w.writerow(row)
    return s.getvalue()

def removeNull(line): #Removing the missing values for calculating averages
	newData = tuple(0.0 if x==-9999 else x for x in line)
	return newData

def fillMissingValues(line): #function that fills the missing values with the averages
 	key = line[6]+line[7][0:6]
 	newList = list(line)
 	if(newList[1]=="-9999"):
 		newList[1]= csv_data_as_prcp_map.get(key)
 	if(newList[2]=="-9999"):
 		newList[2] = csv_data_as_snwd_map.get(key)
 	if(newList[3]=="-9999"):
 		newList[3] = csv_data_as_snow_map.get(key)
 	if(newList[4]=="-9999"):
 		newList[4] = csv_data_as_tmax_map.get(key)
 	if(newList[5]=="-9999"):
 		newList[5] = csv_data_as_tmin_map.get(key)
 	newTuple = tuple(newList)
 	return newTuple

if __name__ == "__main__":
	start = time.time()
	sc = SparkContext(appName = "Extreme_Weather_Analyzer")
	#conf = SparkConf().setAppName("Extreme")
	#conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	#sc = SparkContext(conf=conf)

	csv_data = sc.textFile("/gpfs/courses/cse603/students/suryasid/Data_Four/*.csv",use_unicode=False)\ # Load the dataset and split the data to get the required columns
					 .map(lambda x: (x.split(",")[0],x.split(",")[1],x.split(",")[2],x.split(",")[3],x.split(",")[4],x.split(",")[5],x.split(",")[6],x.split(",")[8]))\
					 .filter(lambda line: "EXTREME" not in line)
	
	csv_data_as_kv_snwd = csv_data.map(lambda x: (x[6]+x[7][0:6],float(x[2])))\ # Generating the key for each station.
							 .map(removeNull)\ # removing null values
							 .mapValues(lambda x:(x,1))\ 
							 .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))#calculating averages for that particular station 
	csv_data_as_snwd_map = csv_data_as_kv_snwd.map(lambda (key,xy):(key,xy[0]/xy[1])).collectAsMap()#creating a hashmap for each feature
	csv_data_as_kv_prcp = csv_data.map(lambda x: (x[6]+x[7][0:6],float(x[1])))\
							 .map(removeNull)\
							 .mapValues(lambda x:(x,1))\
							 .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
	csv_data_as_prcp_map = csv_data_as_kv_prcp.map(lambda (key,xy):(key, xy[0]/xy[1])).collectAsMap()
	csv_data_as_kv_snow = csv_data.map(lambda x: (x[6]+x[7][0:6],float(x[3])))\
							 .map(removeNull)\
							 .mapValues(lambda x:(x,1))\
							 .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
	csv_data_as_snow_map = csv_data_as_kv_snow.map(lambda (key,xy):(key, xy[0]/xy[1])).collectAsMap()
	csv_data_as_kv_tmax = csv_data.map(lambda x: (x[6]+x[7][0:6],float(x[4])))\
							 .map(removeNull)\
							 .mapValues(lambda x:(x,1))\
							 .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
	csv_data_as_tmax_map = csv_data_as_kv_tmax.map(lambda (key,xy):(key, xy[0]/xy[1])).collectAsMap()
	csv_data_as_kv_tmin = csv_data.map(lambda x: (x[6]+x[7][0:6],float(x[5])))\
							 .map(removeNull)\
							 .mapValues(lambda x:(x,1))\
							 .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
	csv_data_as_tmin_map = csv_data_as_kv_tmin.map(lambda (key,xy):(key, xy[0]/xy[1])).collectAsMap()
	csv_data.final = csv_data.map(fillMissingValues)\
				 .map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5]))\#Extracting just the features
				 .map(lambda (x,y,z,w,u,v): gen_row([x,y,z,w,u,v]))# Generating list from tuple
	data_parsed = csv_data.final.map(parsePoint) #Parsing the RDD and converting it into LabeledRDD
	data_parsed.cache()#Caching the data
	model = SVMWithSGD.train(data_parsed, iterations=100)#Training the data with SVM
	labelsAndPreds = data_parsed.map(lambda p: (p.label, model.predict(p.features))) #Predicting the labels from the trained model
	trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(data_parsed.count())#Calculating the error
	print("Training Error = " + str(trainErr))
	end = time.time()
	print end - start
	sc.stop()	
