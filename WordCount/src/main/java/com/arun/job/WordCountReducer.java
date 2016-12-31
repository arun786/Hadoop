package com.arun.job;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		/* i/p for the reducer will be something like this */
		/* portland - {3,4,5} */
		int count = 0;
		for (IntWritable value : values) {
			count = count + value.get();
		}
		context.write(new Text(key), new IntWritable(count));
	}
}
