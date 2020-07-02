package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 22 - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	String output = "";
    	for (Text value : values) {
    		output = output.concat(value+" ");
    	}
    context.write(new Text(output),NullWritable.get());
    }
    
}
