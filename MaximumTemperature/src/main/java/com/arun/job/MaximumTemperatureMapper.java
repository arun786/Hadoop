package com.arun.job;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaximumTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final int MISSING = 9999;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/*value which will be read by mapper from a file will be as below
		 * 0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999
		 * 				  1901 this is the year													  -00781 the first four including the sign is temperature and 1 is the quality
		 * 				  index																		index
		 * 					15 - 19																	87 to 92 quality is 92 to 93
		*/
		
		String lines = value.toString();
		String year = lines.substring(15,19);
		String quality = lines.substring(92,93);
		int temperature;
		
		if(lines.charAt(87) == '+'){
			temperature = Integer.parseInt(lines.substring(88, 92));
		}else{
			temperature = Integer.parseInt(lines.substring(87, 92));
		}
		
		if(temperature != MISSING && quality.matches("[01459]")){
			context.write(new Text(year), new IntWritable(temperature));
		}
	}

}
