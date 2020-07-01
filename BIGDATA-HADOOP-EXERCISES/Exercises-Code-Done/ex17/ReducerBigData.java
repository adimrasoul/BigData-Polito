package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,  // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    float max = Float.MIN_VALUE;
    for (FloatWritable value:values) {
    	if (value.get() > max) {
    		max= value.get();
    		}
    }
    context.write(key,new FloatWritable(max));
    }
}
