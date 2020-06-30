package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<NullWritable, // Input key type
		DateIncome, // Input value type
		Text, // Output key type
		FloatWritable> { // Output value type

	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers have the
	// same key (NullWritable.get())
	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<DateIncome> values, // Input value type
			Context context) throws IOException, InterruptedException {
		
		DateIncome globalTop1 = new DateIncome();
		globalTop1.setDate(null);
		globalTop1.setIncome(Float.MIN_VALUE);
		
		
		
		for(DateIncome value:values) {
			//globalTop1.setDate(value.getDate());
			//globalTop1.setIncome(value.getIncome());
			if(value.getIncome() > globalTop1.getIncome()  ||
					globalTop1.getIncome() == value.getIncome() && 
					globalTop1.getDate().compareTo(value.getDate())> 0 ) 
			{
				
				globalTop1 = new DateIncome();
				globalTop1.setDate(value.getDate());
				globalTop1.setIncome(value.getIncome());	
			}
			
		}
	context.write(new Text(globalTop1.getDate()),new FloatWritable(globalTop1.getIncome()));
	}
	
}
		// Emit pair (date, income) associated with top 1 income


