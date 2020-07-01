package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,  // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    	
	
	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers as the 
	// same key (NullWritable.get())
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String tempDate;
    	float tempValue;
    	DateValue globalTop1;
    	DateValue globalTop2;
    	globalTop1 = null;
    	globalTop2 = null;
    	
    	for(Text value:values) {
    		String[] data = value.toString().split("_");
    		tempDate = new String(data[0]);
    		tempValue = Float.parseFloat(data[1]);
    		
    	if(globalTop1==null ||
    			tempValue>globalTop1.value) {
    		globalTop2 = globalTop1;
    		globalTop1 = new DateValue();
    		globalTop1.date = new String(tempDate);
    		globalTop1.value = new Float(tempValue);    		
    	}
    	else {
    		if(globalTop2 == null ||
    				tempValue > globalTop2.value) {
    			globalTop2 = new DateValue();
    			globalTop2.date = new String(tempDate);
        		globalTop2.value = tempValue;
    		}
    	}
    	}
    	context.write(new Text(globalTop1.date),new FloatWritable(globalTop1.value));
    	context.write(new Text(globalTop2.date), new FloatWritable(globalTop2.value));
    }
}
