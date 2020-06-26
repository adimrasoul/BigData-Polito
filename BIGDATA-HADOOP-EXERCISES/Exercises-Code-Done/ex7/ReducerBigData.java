package it.polito.bigdata.hadoop.exercise;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				Text, // Input value typeF
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {
			String sentenceNo = new String();
			for(Text value:values) {
				sentenceNo = sentenceNo.concat(value+", ");
			}
			context.write(key,new Text(sentenceNo));

	}
	}
