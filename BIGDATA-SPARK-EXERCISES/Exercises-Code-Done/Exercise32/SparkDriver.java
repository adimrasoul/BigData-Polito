package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #32")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//read input
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		//map only pm values as double
		JavaRDD <Double> pmValuesRDD = inputRDD.map( lineInput ->
				{
					String[] fields =lineInput.split(",");
					Double pmValue = new Double(fields[2]);
					return pmValue;
				});
		//take the top of values
		List<Double> topPmValue = pmValuesRDD.top(1);
		
		//print the output
		for(Double value:topPmValue) {
			System.out.println(value);
		}
		//close the session
	sc.close();
	}
}
