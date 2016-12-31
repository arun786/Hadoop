package com.arun.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Input in the value field will be as below where the first is code,
		 * city and then the institute
		 */
		/* OR, Portland, abhyaas hadoop technology */
		/*
		 * Requirement is to count the number of words in Institute for a
		 * particular City
		 */

		String line = value.toString();
		String[] result = line.split(",");
		String code = result[0]; // Code = OR
		String city = result[1]; // City = Portland
		String institute = result[2]; // Institute = abhyaas hadoop technology

		StringTokenizer token = new StringTokenizer(institute);
		int count = 0;
		while (token.hasMoreTokens()) {
			token.nextToken();
			count++;
		}

		context.write(new Text(city), new IntWritable(count)); // this will be
																// input for the
																// Reducer.
		/*
		 * for the above example, it will be Portland, 3 as the o/p for mapper
		 */

	}

}
