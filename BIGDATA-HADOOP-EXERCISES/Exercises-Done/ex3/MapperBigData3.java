package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 3 - Mapper
 */
class MapperBigData3 extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
	private static Double PM10Threshold = new Double(50);
	
    protected void map(
            Text key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	String[] words = key.toString().split(",");
    	String sensorId = words[0];
    	Double SensorVal = new Double(value.toString());
    	if(SensorVal>=50) {
    		context.write(new Text(sensorId), new IntWritable(1));
    		
    	}
    }
}
