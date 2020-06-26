package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData5 extends
		Reducer<Text, // Input key type
				FloatWritable, // Input value type
				Text, // Output key type
				FloatWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<FloatWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
			int counter = 0;
			float sum = 0;
			for(FloatWritable value:values) {
				counter=counter+1;
				sum=sum+value.get();
						
			}
			context.write(key,new FloatWritable(sum/counter));
	}
}
