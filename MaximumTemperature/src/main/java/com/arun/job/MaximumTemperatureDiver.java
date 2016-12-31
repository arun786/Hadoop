package com.arun.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MaximumTemperatureDiver extends Configured implements Tool {

	/*
	 * year is at index 15 to 19. temperature is at index 87 to 92. quality is
	 * at index 92 to 93.
	 * 
	 * These indexes are as per substring of string, where startindex is
	 * included and endindex is not included, indexes for a substring starts
	 * from 0
	 * 
	 * Requirement is to find the maximum and minimum temperature for given
	 * years in files where quality is either of the following 01459
	 */

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(MaximumTemperatureDiver.class);
		
		job.setMapperClass(MaximumTemperatureMapper.class);
		job.setReducerClass(MaximumTemperatureReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {

		MaximumTemperatureDiver d = new MaximumTemperatureDiver();
		try {
			int res = ToolRunner.run(d, args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
