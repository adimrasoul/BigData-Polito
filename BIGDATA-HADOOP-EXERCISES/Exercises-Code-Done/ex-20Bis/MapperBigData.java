package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Mapper of a map-only job
 *  */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Writable,         // Output key type
                    NullWritable> {// Output value type
	
	// Define a MultiOutputs object
	private MultipleOutputs<Writable, NullWritable> multiOutSample = null;
	float threshold;
	
	protected void setup(Context context)
	{
		// Create a new MultiOuputs using the context object
		multiOutSample = new MultipleOutputs<Writable, NullWritable>(context);
		threshold = 30;
	}

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each record by using the field separator
    		// fields[0]= sensor id
    		// fields[1]= date
    		// fields[2]= hour:minute
    		// fields[3]= temperature
			String[] fields = value.toString().split(",");
			
			float temp=Float.parseFloat(fields[3]);
			
			if (temp>threshold)
				multiOutSample.write("hightemp", 
						new FloatWritable(temp), NullWritable.get());
			multiOutSample.write("normaltemp", 
						value, NullWritable.get());
				
    }
    
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		// Close the MultiOutputs
		// If you do not close the MultiOutputs object the content of the output
		// files will not be correct
		multiOutSample.close();
	}

    
}
