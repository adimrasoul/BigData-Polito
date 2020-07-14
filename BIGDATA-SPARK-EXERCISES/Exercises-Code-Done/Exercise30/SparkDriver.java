package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read the input elements
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		//apply filter of 'google' word  on the inputRDD
		
		JavaRDD <String> googleRDD = inputRDD.filter(inputLine ->
										inputLine.toLowerCase().contains("google"));
		//Save the content as Text File
		googleRDD.saveAsTextFile(outputPath);
		
		//close the session
		sc.close();
	}
}
