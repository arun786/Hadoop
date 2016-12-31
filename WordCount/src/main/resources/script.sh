#!/bin/bash
hadoop fs -rm -r /Arun/Mapreduce/WordCount/FileToWrite/*
hadoop jar /home/abhyaastech/Arun/Mapreduce/WordCount/WordCount.jar  com.arun.job.WordCountDriver /Arun/Mapreduce/WordCount/FilesToRead /Arun/Mapreduce/WordCount/FileToWrite/OutputFile