package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData5 extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				FloatWritable> {// Output value type
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
			String[] fields= value.toString().split(",");
			String sensorId = fields[0];
			float sensorVal = Float.parseFloat(fields[2]);
			context.write(new Text(sensorId), new FloatWritable(new Float(sensorVal)));
			
	}
}
