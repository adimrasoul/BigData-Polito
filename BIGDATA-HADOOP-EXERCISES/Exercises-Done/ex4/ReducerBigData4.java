package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 4 - Reducer
 */
class ReducerBigData4 extends
		Reducer<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {
			
		String Dates = new String();
		for(Text value:values) {
				if(Dates.length()==0)
					Dates =  new String(value.toString());
				else 
					Dates = Dates.concat(", " + value.toString());
			}
	context.write(new Text(key), new Text(Dates));
	}
}
