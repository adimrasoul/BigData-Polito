package it.polito.bigdata.hadoop.exercise;

import it.polito.bigdata.hadoop.exercise.DriverBigData.myCounter;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Ex. 10 Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				NullWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

	context.getCounter(myCounter.lineNumber).increment(1);

	}
}
