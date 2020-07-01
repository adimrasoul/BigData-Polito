package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Exercise 21 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private ArrayList<String> stopWords;
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		String nextLine;

		
		stopWords=new ArrayList<String>();
		// Open the stopword file (that is shared by means of the distributed 
		// cache mechanism) 
		URI[] urisCachedFiles = context.getCacheFiles();
	
		// This application has one single single cached file. 
		// Its path is stored in urisCachedFiles[0] 
		BufferedReader fileStopWords = new BufferedReader(new 
				FileReader(new File(urisCachedFiles[0].getPath())));
	
		
		// Each line of the file contains one stopword 
		// The stopwords are stored in the stopWords list
		while ((nextLine = fileStopWords.readLine()) != null) {
			stopWords.add(nextLine);
		}
	
		fileStopWords.close();
	}

	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		boolean wordFlag;
    		String[] words = value.toString().split(" ");
    		String textWithoutStopWords = "";
    		
    		for(String word : words) {
    			//if the current word is one of stop words , the algo does not consider it
    			if (stopWords.contains(word)== true) {
    				wordFlag = true;
    			}
    			else {
    				wordFlag =false;
    				}
    			if(wordFlag==false) {
    				textWithoutStopWords = textWithoutStopWords.concat(word+" ");
    			}
    		}
    		context.write(NullWritable.get(),new Text(textWithoutStopWords));
    }
}
