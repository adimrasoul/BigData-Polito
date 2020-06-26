package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData6 extends
		Reducer<Text, // Input key type
				FloatWritable, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<FloatWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
			double min=Double.MAX_VALUE;
			double max= Double.MIN_VALUE;
			for(FloatWritable value:values) {
				if(value.get()<min) {
					min = value.get();
				}
				if (value.get()>max) {
					max = value.get();
				}
				
			}
			context.write(key,new Text("Min is: "+ min +"Max is: "+ max) );
					
}
}
