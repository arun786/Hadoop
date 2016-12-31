#!/bin/bash
hadoop fs -rm -r /Arun/Mapreduce/MaxTemperature/FilesToWrite/*
hadoop jar MaximumTemperature.jar com.arun.job.MaximumTemperatureDiver /Arun/Mapreduce/MaxTemperature/FilesToRead /Arun/Mapreduce/MaxTemperature/FilesToWrite/output