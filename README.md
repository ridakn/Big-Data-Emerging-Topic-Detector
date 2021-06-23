# Big-Data-Emerging-Topic-Detector
Built an emerging topic detector to analyse sentiment of tweets under the emerging topic using Apache Spark and Twitter API.

## Introduction

<p align = "justify"> 
Data Analytics for business is becoming increasingly important for various reasons; one of the reasons is that this data can inform various crucial business decisions and operations in all departments of a company. Therefore, accurately analyzing this data has become very important. However, the data has become so huge, that it's getting difficult to process it in traditional ways which causes delays and therefore affects the accuracy of the analytics. Slower data analytics systems are less accurate which is where real time analytics comes in; analyzing data as it's coming in or being able to successfully analyze streaming data has become crucial. Hadoop does batch processing i.e. process on old data. The need for the hour is to be able to stream and process near real time data which leads to the main advantage of Apache Spark. Spark streaming can be used for successful and fast near-real time analysis. </p>
  
<p align = "justify"> 
In this project, the main objective was to develop an emerging topic detector from Twitter streaming. We had to stream data from Twitter stream using Spark streaming and develop an emerging topic detector. An emerging topic is defined as a topic that wasn't being talked about in a previous time window but is popular now therefore it's "emerging". Once, the emerging topic was chosen, the tweets contain it would be run through a sentiment analyzer to infer their sentiment. The output would be the emerging topic, their tweets and respective sentiments. </p>

## Method

<img width="771" alt="Screen Shot 2021-06-24 at 12 56 54 AM" src="https://user-images.githubusercontent.com/32781544/123167047-eb84dc80-d42a-11eb-9ba8-f64d98617feb.png">
