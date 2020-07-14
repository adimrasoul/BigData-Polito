package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #33")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read data
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		//map all the pm values
		JavaRDD<Double> pmValuesRDD = inputRDD.map(inputLine ->
				{
				String[] fields = inputLine.split(",");
				Double pmValue = new Double(fields[2]);
				return pmValue;
			
				});
		//take the three top Values
		List<Double> topValues = pmValuesRDD.top(3);
		
		//print the output
		for(Double value:topValues) {
			System.out.println(value);
		}
	}
}
