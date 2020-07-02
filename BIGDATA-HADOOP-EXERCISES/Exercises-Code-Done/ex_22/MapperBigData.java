package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 22 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, 		  // Input key type
                    Text, 		  // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	
	String specifiedUser;

	protected void setup(Context context) {
		// Retrieve the value of the user of interest
        specifiedUser=context.getConfiguration().get("userInput");
	}

    
    protected void map(
            LongWritable key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		String[] words = value.toString().split(",");
    		if (specifiedUser.compareTo(words[0].toLowerCase())==0) {
    			context.write(NullWritable.get(), new Text(words[1]));
    		}
    		if(specifiedUser.compareTo(words[1].toLowerCase())==0) {
    			context.write(NullWritable.get(),new Text(words[0]));
    		}
            
    }
}
