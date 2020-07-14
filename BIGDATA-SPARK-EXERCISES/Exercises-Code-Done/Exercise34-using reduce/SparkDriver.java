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
		
		//read the input
		JavaRDD <String> inputRDD = sc.textFile(inputPath).cache();
	
		//map only pmvalues
		JavaRDD<Double> pmValuesRDD = inputRDD.map( inputLine ->
				{
					String[] fields = inputLine.split(",");
					Double pmValue = new Double(fields[2]);
					return pmValue;
				});
		//take the top1
		//List<Double> topValues = pmValuesRDD.top(1);
		Double pmTopList = pmValuesRDD.reduce((pm1,pm2)->
		{
		if(pm1>pm2) {
			return pm1;
		}
		else
		{
			return pm2;
		}
		});
		//find the line associated with the found max
		JavaRDD<String> maxLineRDD = inputRDD.filter(inputLine ->
		{
			String[] parts = inputLine.split(",");
			Double pmValue = new Double(parts[2]);
			if(pmValue.equals(pmTopList)) {
				return true;
				}
			else {
				return false;
			}
		}	);
		//save output
		maxLineRDD.saveAsTextFile(outputPath);
		
		//close session
		sc.close();
	}
}
