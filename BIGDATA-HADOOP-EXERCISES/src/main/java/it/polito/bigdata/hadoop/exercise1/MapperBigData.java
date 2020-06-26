package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each sentence in words. Use whitespace(s) as delimiter (=a
		// space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] fields= value.toString().split("//s");
		
		for (String field:fields) {
			String cleanedWord = field.toLowerCase();
			if(cleanedWord.compareTo("and")== 0 && cleanedWord.compareTo("or")==0 && 
					cleanedWord.compareTo("not")==0) {
				context.write(new Text(cleanedWord), new Text(key));
			}
		}
	}
}
