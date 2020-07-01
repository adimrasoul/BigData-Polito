package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first data format
 */
class MapperType1BigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		FloatWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each record by using the field separator
		// fields[0]= sensor id
		// fields[1]= date
		// fields[2]= hour:minute
		// fields[3]= temperature
		String[] fields = value.toString().split(",");
		String date = fields[1];
		float temp = Float.parseFloat(fields[3]);
		
		context.write(new Text(date), new FloatWritable(temp));
	}

}
