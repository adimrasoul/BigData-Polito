package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #34")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read input
		JavaRDD <String> inputRDD = sc.textFile(inputPath).cache();
		
		//map only pm values
		JavaRDD <Double> pmValuesRDD = inputRDD.map(inputLine ->
		{
			String[] fields = inputLine.split(",");
			Double pmValue = new Double(fields[2]);
			return pmValue;
		}
				);
		//find the top
		List<Double> topPM = pmValuesRDD.top(1);
		//filter the line of top value
		JavaRDD <String> lineTopRDD = inputRDD.filter(inputLine ->
		{
			String[] fields = inputLine.split(",");
			Double pmValue = new Double(fields[2]);
			if (pmValue.equals(topPM.get(0))) 
				return true;
			else
				return false;
			});
		JavaRDD<String> dateTopRDD = lineTopRDD.map(inputLine ->
					{
						String[] parts = inputLine.split(",");
					String Date = new String(parts[1]);
					return Date;		
					});
		//save
		dateTopRDD.saveAsTextFile(outputPath);
		//close
		sc.close();
	}
}
