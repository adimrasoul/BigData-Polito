package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 9 - Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				IntWritable> {// Output value type
	HashMap<String,Integer> countWords;
	protected void setup(Context context) {
		countWords = new HashMap<String,Integer>();
	} 
	
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
		Integer frequency;
				String[] words = value.toString().split("\\s+");
		
				for(String word:words) {
					String cleanedWord = word.toLowerCase();
					frequency =countWords.get(cleanedWord);
					
					if(frequency==null) {
						countWords.put(new String(cleanedWord),new Integer(1));
											
					}
					else {
						frequency = frequency+1;
						countWords.put(new String(cleanedWord), new Integer(frequency));
					}
				}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Entry <String,Integer> pairValue:countWords.entrySet()) {
		context.write(new Text(pairValue.getKey()),
				new IntWritable(pairValue.getValue()));
	}
		}
	
}

	